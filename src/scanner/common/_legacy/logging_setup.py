"""
Logging Setup mit Rotation für Scanner Projekt
"""
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging(log_path="logs/scanner.log", level="INFO"):
    """
    Konfiguriert Logging mit Rotation und Console Output
    
    Args:
        log_path (str): Pfad zur Log-Datei
        level (str): Logging Level (DEBUG, INFO, WARNING, ERROR)
    
    Returns:
        logging.Logger: Konfigurierter Logger
    """
    # logs Ordner erstellen falls nicht vorhanden
    log_dir = Path(log_path).parent
    log_dir.mkdir(exist_ok=True)
    
    # Logger erstellen/konfigurieren
    logger = logging.getLogger("scanner")
    logger.setLevel(getattr(logging, level.upper()))
    
    # Bestehende Handler entfernen (wichtig für reloads)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Format definieren
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Rotating File Handler (2MB, 5 Backups)
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=2*1024*1024,  # 2MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

if __name__ == "__main__":
    # Test
    logger = setup_logging()
    logger.info("Logging setup test - OK")
    logger.warning("Test warning")
    logger.error("Test error")
