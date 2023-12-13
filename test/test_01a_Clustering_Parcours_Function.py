# Do the same for the following functions
# Functions in src/user_functions.py and tests in tests/test_user_functions.py
import sys
    # Tests (copy to tests/test_user_functions.py)
import pytest
import io
sys.path+=['src']
#sys.path.append('/src')

print('VINCENT' + str(sys.path))

from 01a_Clustering_Parcours_Function import *
#from src import Exerciceuser_functions as Ex


def test_email_with_user_input_no_at_sign(monkeypatch):
    monkeypatch.setattr('sys.stdin', io.StringIO('petra.adaltas.com'))
    assert get_email_from_input() is None


