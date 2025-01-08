from datetime import datetime
from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.orm import declarative_base

# Create a base class for declarative models
Base = declarative_base()

class BitcoinPrice(Base):
    """Defines the table at database."""
    __tablename__ = 'bitcoin_prices'

    id = Column(Integer, primary_key=True, autoincrement=True)
    value = Column(Float, nullable=False)
    criptocurrency = Column(String(length=50), nullable=False)
    currency = Column(String(length=10), nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.now)