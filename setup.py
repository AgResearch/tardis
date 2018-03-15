#!/usr/bin/env python
#
# setuptools setup script for tardis

from setuptools import setup, find_packages

setup(name='tardis',
      use_scm_version=True,
      setup_requires=['setuptools_scm', 'setuptools_scm_git_archive'],
      description='Front-end for bioinformatics HPC job submission',
      author='Alan McCulloch',
      author_email='alan.mcculloch@agresearch.co.nz',
      url='https://github.com/AgResearch/tardis',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Environment :: Console',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 2.7',
          'Topic :: Scientific/Engineering :: Bio-Informatics',
      ],
      packages=find_packages(),
      entry_points={
        'console_scripts': [
            'tardis = tardis.__main__:main',
            'tardish = tardis.tardish:main',
        ],
      },
      package_data={
          'doc': ['doc/tardis.pdf'],
          'test': ['*'],
      },
      license='GPLv2',
      install_requires=[
          'biopython',
          'pyzmq',
      ],
      python_requires='>=2.7, <3',
     )
