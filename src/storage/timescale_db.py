import asyncio
import asyncpg
import pandas as pd
from typing import Dict, List, Optional, Any, Union, Tuple
import json
from datetime import datetime, timezone

from src.utils.logger import get_storage_logger
from src.utils.config import DATABASE_URL

class TimescaleDBClient:
    """
    Client to interact with TimescaleDB (PostgreSQL extension)
    for time-series financial data
    """
    
    def __init__(self, connection_url: Optional[str] = None):
        self.logger = get_storage_logger("timescale")
        self.connection_url = connection_url or DATABASE_URL
        self.pool = None
    
    async def init_pool(self, min_size: int = 5, max_size: int = 20) -> None:
        """
        Initialize connection pool
        
        Args:
            min_size (int): Minimum number of connections
            max_size (int): Maximum number of connections
        """
        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.connection_url,
                min_size=min_size,
                max_size=max_size,
                command_timeout=60
            )
            self.logger.info(f"Connected to TimescaleDB with pool size {min_size}-{max_size}")
            
            # Initialize tables
            await self._init_tables()
            
        except Exception as e:
            self.logger.error(f"Error connecting to TimescaleDB: {str(e)}")
            raise
    
    async def close_pool(self) -> None:
        """Close the connection pool"""
        if self.pool:
            await self.pool.close()
            self.logger.info("Closed TimescaleDB connection pool")
    
    async def _init_tables(self) -> None:
        """Initialize required tables and hypertables"""
        async with self.pool.acquire() as conn:
            # Check if TimescaleDB extension is installed
            try:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            except Exception as e:
                self.logger.error(f"Error creating TimescaleDB extension: {str(e)}")
                self.logger.warning("Continuing without TimescaleDB extension. Time-series optimizations will not be available.")
            
            # Create OHLCV table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv (
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    open DOUBLE PRECISION NOT NULL,
                    high DOUBLE PRECISION NOT NULL,
                    low DOUBLE PRECISION NOT NULL,
                    close DOUBLE PRECISION NOT NULL,
                    volume DOUBLE PRECISION,
                    source TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (symbol, date)
                );
            """)
            
            # Try to convert to hypertable if it's not already
            try:
                await conn.execute("""
                    SELECT create_hypertable('ohlcv', 'date', if_not_exists => TRUE);
                """)
            except Exception as e:
                self.logger.warning(f"Could not convert to hypertable: {str(e)}")
            
            # Create fundamentals table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS fundamentals (
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    market_cap DOUBLE PRECISION,
                    pe_ratio DOUBLE PRECISION,
                    eps DOUBLE PRECISION,
                    dividend_yield DOUBLE PRECISION,
                    book_value DOUBLE PRECISION,
                    price_to_book DOUBLE PRECISION,
                    revenue DOUBLE PRECISION,
                    net_income DOUBLE PRECISION,
                    source TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (symbol, date)
                );
            """)
            
            # Try to convert to hypertable if it's not already
            try:
                await conn.execute("""
                    SELECT create_hypertable('fundamentals', 'date', if_not_exists => TRUE);
                """)
            except Exception as e:
                self.logger.warning(f"Could not convert fundamentals to hypertable: {str(e)}")
            
            # Create dividends table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dividends (
                    symbol TEXT NOT NULL,
                    ex_date DATE NOT NULL,
                    payment_date DATE,
                    amount DOUBLE PRECISION NOT NULL,
                    currency TEXT,
                    source TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (symbol, ex_date)
                );
            """)
            
            # Create news table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS news (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    title TEXT NOT NULL,
                    content TEXT,
                    source TEXT,
                    url TEXT,
                    sentiment DOUBLE PRECISION,
                    symbols TEXT[],
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            
            # Create symbols table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS symbols (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    exchange TEXT,
                    sector TEXT,
                    industry TEXT,
                    country TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    last_updated TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            
            self.logger.info("TimescaleDB tables initialized")
    
    async def insert_ohlcv(self, df: pd.DataFrame, conflict_action: str = 'update') -> int:
        """
        Insert OHLCV data into the database
        
        Args:
            df (pd.DataFrame): DataFrame with OHLCV data
            conflict_action (str): Action on conflict ('update', 'ignore')
            
        Returns:
            int: Number of rows inserted
        """
        if not self.pool:
            await self.init_pool()
        
        if df.empty:
            self.logger.warning("Empty DataFrame provided for OHLCV insertion")
            return 0
        
        required_columns = ['symbol', 'date', 'open', 'high', 'low', 'close']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            self.logger.error(f"Missing required columns for OHLCV insertion: {missing_columns}")
            return 0
        
        try:
            # Convert date to string if it's datetime
            if pd.api.types.is_datetime64_any_dtype(df['date']):
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            # Prepare values for insertion
            records = df[['symbol', 'date', 'open', 'high', 'low', 'close']].to_dict('records')
            
            # Add volume if available
            if 'volume' in df.columns:
                for i, record in enumerate(records):
                    record['volume'] = df['volume'].iloc[i]
            
            # Add source if available
            if 'source' in df.columns:
                for i, record in enumerate(records):
                    record['source'] = df['source'].iloc[i]
            
            # Build ON CONFLICT action
            if conflict_action == 'update':
                on_conflict = """
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    source = EXCLUDED.source,
                    created_at = NOW()
                """
            else:  # 'ignore'
                on_conflict = "ON CONFLICT (symbol, date) DO NOTHING"
            
            # Insert data in chunks to avoid memory issues
            chunk_size = 1000
            total_inserted = 0
            
            async with self.pool.acquire() as conn:
                for i in range(0, len(records), chunk_size):
                    chunk = records[i:i+chunk_size]
                    
                    # Prepare values list for insertion
                    values = []
                    for record in chunk:
                        row = (
                            record['symbol'],
                            record['date'],
                            record['open'],
                            record['high'],
                            record['low'],
                            record['close'],
                            record.get('volume'),
                            record.get('source')
                        )
                        values.append(row)
                    
                    # Execute insertion
                    result = await conn.executemany(f"""
                        INSERT INTO ohlcv (symbol, date, open, high, low, close, volume, source)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        {on_conflict}
                    """, values)
                    
                    # Update count of inserted rows
                    if result:
                        inserted = len(chunk)
                    else:
                        inserted = 0
                    
                    total_inserted += inserted
                    self.logger.debug(f"Inserted chunk of {inserted} OHLCV records")
            
            self.logger.info(f"Successfully inserted {total_inserted} OHLCV records")
            return total_inserted
            
        except Exception as e:
            self.logger.error(f"Error inserting OHLCV data: {str(e)}")
            return 0
    
    async def insert_fundamentals(self, df: pd.DataFrame, conflict_action: str = 'update') -> int:
        """
        Insert fundamental data into the database
        
        Args:
            df (pd.DataFrame): DataFrame with fundamental data
            conflict_action (str): Action on conflict ('update', 'ignore')
            
        Returns:
            int: Number of rows inserted
        """
        if not self.pool:
            await self.init_pool()
        
        if df.empty:
            self.logger.warning("Empty DataFrame provided for fundamentals insertion")
            return 0
        
        required_columns = ['symbol', 'date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            self.logger.error(f"Missing required columns for fundamentals insertion: {missing_columns}")
            return 0
        
        try:
            # Convert date to string if it's datetime
            if pd.api.types.is_datetime64_any_dtype(df['date']):
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            # Prepare values for insertion
            fundamental_columns = [
                'market_cap', 'pe_ratio', 'eps', 'dividend_yield', 
                'book_value', 'price_to_book', 'revenue', 'net_income', 'source'
            ]
            
            # Keep only columns that exist in the DataFrame
            available_columns = ['symbol', 'date'] + [col for col in fundamental_columns if col in df.columns]
            
            records = df[available_columns].to_dict('records')
            
            # Build ON CONFLICT action
            if conflict_action == 'update':
                update_fields = []
                for col in fundamental_columns:
                    if col in available_columns:
                        update_fields.append(f"{col} = EXCLUDED.{col}")
                
                update_fields.append("created_at = NOW()")
                on_conflict = f"""
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    {', '.join(update_fields)}
                """
            else:  # 'ignore'
                on_conflict = "ON CONFLICT (symbol, date) DO NOTHING"
            
            # Insert data in chunks to avoid memory issues
            chunk_size = 1000
            total_inserted = 0
            
            async with self.pool.acquire() as conn:
                for i in range(0, len(records), chunk_size):
                    chunk = records[i:i+chunk_size]
                    
                    # For each record in the chunk
                    for record in chunk:
                        # Create placeholders and values dynamically based on available columns
                        placeholders = ', '.join(f"${i+1}" for i in range(len(record)))
                        columns = ', '.join(record.keys())
                        values = list(record.values())
                        
                        # Execute insertion
                        await conn.execute(f"""
                            INSERT INTO fundamentals ({columns})
                            VALUES ({placeholders})
                            {on_conflict}
                        """, *values)
                    
                    total_inserted += len(chunk)
                    self.logger.debug(f"Inserted chunk of {len(chunk)} fundamental records")
            
            self.logger.info(f"Successfully inserted {total_inserted} fundamental records")
            return total_inserted
            
        except Exception as e:
            self.logger.error(f"Error inserting fundamental data: {str(e)}")
            return 0
    
    async def get_ohlcv(self, symbol: str, start_date: Optional[str] = None, 
                      end_date: Optional[str] = None, limit: int = 1000) -> pd.DataFrame:
        """
        Get OHLCV data from the database
        
        Args:
            symbol (str): Symbol to fetch data for
            start_date (Optional[str]): Start date in YYYY-MM-DD format
            end_date (Optional[str]): End date in YYYY-MM-DD format
            limit (int): Maximum number of rows to return
            
        Returns:
            pd.DataFrame: DataFrame with OHLCV data
        """
        if not self.pool:
            await self.init_pool()
        
        try:
            query = """
                SELECT symbol, date, open, high, low, close, volume, source
                FROM ohlcv
                WHERE symbol = $1
            """
            
            params = [symbol]
            
            if start_date:
                query += " AND date >= $" + str(len(params) + 1)
                params.append(start_date)
            
            if end_date:
                query += " AND date <= $" + str(len(params) + 1)
                params.append(end_date)
            
            query += " ORDER BY date DESC LIMIT $" + str(len(params) + 1)
            params.append(limit)
            
            async with self.pool.acquire() as conn:
                records = await conn.fetch(query, *params)
                
                if not records:
                    self.logger.info(f"No OHLCV data found for {symbol}")
                    return pd.DataFrame()
                
                # Convert to DataFrame
                df = pd.DataFrame(records, columns=[
                    'symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'source'
                ])
                
                self.logger.info(f"Retrieved {len(df)} OHLCV records for {symbol}")
                return df
                
        except Exception as e:
            self.logger.error(f"Error retrieving OHLCV data: {str(e)}")
            return pd.DataFrame()
    
    async def get_symbols(self, exchange: Optional[str] = None, 
                         sector: Optional[str] = None, 
                         active_only: bool = True) -> pd.DataFrame:
        """
        Get symbols from the database
        
        Args:
            exchange (Optional[str]): Filter by exchange
            sector (Optional[str]): Filter by sector
            active_only (bool): Include only active symbols
            
        Returns:
            pd.DataFrame: DataFrame with symbols
        """
        if not self.pool:
            await self.init_pool()
        
        try:
            query = """
                SELECT symbol, name, exchange, sector, industry, country, is_active
                FROM symbols
                WHERE 1=1
            """
            
            params = []
            
            if exchange:
                query += " AND exchange = $" + str(len(params) + 1)
                params.append(exchange)
            
            if sector:
                query += " AND sector = $" + str(len(params) + 1)
                params.append(sector)
            
            if active_only:
                query += " AND is_active = $" + str(len(params) + 1)
                params.append(active_only)
            
            query += " ORDER BY symbol"
            
            async with self.pool.acquire() as conn:
                records = await conn.fetch(query, *params)
                
                if not records:
                    self.logger.info("No symbols found with the given criteria")
                    return pd.DataFrame()
                
                # Convert to DataFrame
                df = pd.DataFrame(records, columns=[
                    'symbol', 'name', 'exchange', 'sector', 'industry', 'country', 'is_active'
                ])
                
                self.logger.info(f"Retrieved {len(df)} symbols")
                return df
                
        except Exception as e:
            self.logger.error(f"Error retrieving symbols: {str(e)}")
            return pd.DataFrame()
