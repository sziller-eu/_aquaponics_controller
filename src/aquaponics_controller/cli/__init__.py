"""Administrative CLI package for the Aquaponics controller.

Exports the main entry point and send_stop_request function.
"""

from aquaponics_controller.cli.main import main, send_stop_request

__all__ = ["main", "send_stop_request"]

