import setuptools

setuptools.setup(name='slurm-jupyter-run',
      version='1.0.2',
      author='Kasper Munch',
      description='Utility for notebooks on a slurm cluster.',
      # long_description=long_description,
      long_description_content_type="text/markdown",
      url='https://github.com/kaspermunch/slurm-jupyter-run',
      packages=setuptools.find_packages(),
      python_requires='>=3.6',
      install_requires=[
      'nbconvert>=5.6',
      ])