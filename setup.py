from setuptools import setup
from os.path import join, dirname

setup(name="pyrecon",
      version="0.3.0",
      description="Python implementation of Recon file format and utilities",
      long_description=open(join(dirname(__file__), 'README.md')).read(),
      author="Michael Tiller",
      author_email="michael.tiller@gmail.com",
      license="MIT",
      url="http://github.com/xogeny/recon/",
      scripts=['scripts/meld_info', 'scripts/wall_info',
               'scripts/wall2meld', 'scripts/dsres2meld'],
      packages=['recon'],
      include_package_data=True,
      zip_safe=False)
