#python setup.py build_ext --inplace

import setuptools  # important
from distutils.core import setup
from Cython.Build import cythonize
import numpy

setup(
    ext_modules = cythonize("_finance_ext.pyx"),
    include_dirs=[numpy.get_include()]
)
