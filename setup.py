from setuptools import find_packages, setup


if __name__ == '__main__':
    setup(name='improve_trainer',
          version='0.1',
          description='v6 Decision API',
          author='Justin Chapweske',
          author_email='',
          url='https://github.com/improve-ai/trainer/tree/v6',
          packages=find_packages(
              where="src",
              exclude=['*tests*', '*node_modules*', '*serverless*']),
          package_dir={"": "src"},
        )
