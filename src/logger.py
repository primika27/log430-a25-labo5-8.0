"""
Logger utility
SPDX-License-Identifier: LGPL-3.0-or-later
"""
import logging

class Logger:
    @staticmethod
    def get_instance(name: str):
        return logging.getLogger(name)
