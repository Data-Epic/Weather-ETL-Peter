import logging
import os
import sys
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from dags.config import DATABASE_URL
from dags.models import Base

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)


@contextmanager
def get_db() -> Generator[Session, None, None]:
    """
    Function to create a database session

    Yields:
    Session: A sqlalchemy session object

    """

    SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=create_engine(DATABASE_URL)
    )
    db = SessionLocal()

    try:
        logger.info("Database session opened")
        yield db

    except Exception as e:
        logger.error(f"Error occurred during database operation: {str(e)}")
        raise

    finally:
        logger.info("Closing database session")
        db.close()
