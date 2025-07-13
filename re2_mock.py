"""
Mock re2 module for Python 3.13+ compatibility
Falls back to Python's built-in re module when google-re2 cannot be compiled
This provides the same interface as google-re2 but uses the standard library
"""

import re as _re
import warnings

# Suppress warnings about using fallback
warnings.filterwarnings('ignore', message='.*re2.*')

# Export all standard re2 functions using Python's re module
compile = _re.compile
search = _re.search
match = _re.match
sub = _re.sub
subn = _re.subn
split = _re.split
findall = _re.findall
finditer = _re.finditer
escape = _re.escape
purge = _re.purge

# Re-export re flags
IGNORECASE = _re.IGNORECASE
MULTILINE = _re.MULTILINE
DOTALL = _re.DOTALL
VERBOSE = _re.VERBOSE
UNICODE = _re.UNICODE

# RE2-specific flags (map to re equivalents)
I = IGNORECASE
M = MULTILINE
S = DOTALL
X = VERBOSE
U = UNICODE

# Module info
__version__ = "1.0.0-mock"
__author__ = "Mock Implementation"

# RE2-specific functions
def set_fallback_notification(enable):
    """Mock function for RE2 compatibility"""
    pass

def version():
    """Return mock version"""
    return __version__

# Additional RE2 functions that might be called
def fullmatch(pattern, string, flags=0):
    """Full match equivalent"""
    return _re.fullmatch(pattern, string, flags)

def filter(pattern, strings):
    """Filter strings that match pattern"""
    compiled = _re.compile(pattern)
    return [s for s in strings if compiled.search(s)]

def contains(pattern, string):
    """Check if pattern is contained in string"""
    return bool(_re.search(pattern, string))

# Error classes for compatibility
class RE2Error(Exception):
    """Mock RE2 error class"""
    pass

error = RE2Error