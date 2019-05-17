#python setup.py build_ext --inplace

import setuptools  # important
from distutils.core import setup
from Cython.Build import cythonize
import numpy

setup(
    ext_modules = cythonize("shogun\\lib\\adjustment.pyx"),
    include_dirs=[numpy.get_include()]
)

setup(
   ext_modules = cythonize("shogun\\lib\\_int64window.pyx"),
   include_dirs=[numpy.get_include()]
)

setup(
   ext_modules = cythonize("shogun\\lib\\_float64window.pyx"),
   include_dirs=[numpy.get_include()]
)
'''
setup(
    ext_modules = cythonize("shogun/lib/"),
    include_dirs=[numpy.get_include()]
)
'''