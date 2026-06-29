import atheris
import sys
import pydicom

def TestOneInput(data):
    try:
        pydicom.dcmread(data)
    except Exception:
        pass

atheris.Setup(sys.argv, TestOneInput)
atheris.Fuzz()