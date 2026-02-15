"""
Rebalance Engine mit echten Broker-Exports

Modulare Engine für Portfolio-Rebalancing basierend auf ISIN-Matching.
Integriert mit v6 Scoring System und Telegram Alerts.

Author: Trading-Zentrale v6
"""

from .engine import run_rebalance, create_sample_broker_files, print_rebalance_summary, validate_environment
from .holdings_loader import load_broker_holdings, validate_holdings_structure, get_holdings_summary
from .matcher import match_holdings_to_symbols, create_symbol_map_template, get_matching_statistics
from .diff import build_rebalance_plan, group_actions_by_type, calculate_portfolio_metrics, validate_rebalance_inputs
from .formatters import format_rebalance_message, format_summary_message, send_telegram_message, format_matching_summary

__all__ = [
    # Core Engine
    'run_rebalance',
    'create_sample_broker_files',
    'print_rebalance_summary',
    'validate_environment',
    
    # Holdings Loader
    'load_broker_holdings',
    'validate_holdings_structure',
    'get_holdings_summary',
    
    # Matcher
    'match_holdings_to_symbols',
    'create_symbol_map_template',
    'get_matching_statistics',
    
    # Diff Algorithm
    'build_rebalance_plan',
    'group_actions_by_type',
    'calculate_portfolio_metrics',
    'validate_rebalance_inputs',
    
    # Formatters
    'format_rebalance_message',
    'format_summary_message',
    'send_telegram_message',
    'format_matching_summary'
]
