import os
from setuptools import find_packages, setup

SRC_ABS_PATH = os.path.abspath(os.sep.join(str(__file__).split(os.sep)[:-3]))

if __name__ == '__main__':

    gym_base_path = SRC_ABS_PATH.replace('tests/', '')

    setup(name='improve_gym',
          version='0.7',
          description='v7 improve.ai GYM',
          author='Justin Chapweske',
          author_email='',
          url='https://github.com/improve-ai/gym',
          packages=find_packages(
              where=gym_base_path,
              exclude=['*tests*', '*node_modules*', '*serverless*', '*debug*']),
          package_dir={"": gym_base_path},
        )
