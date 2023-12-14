import os
from setuptools import setup
from glob import glob

package_name: str = 'rmcl'
mqtt_package_name: str = package_name + '.mqtt'
node_package_name: str = package_name + '.node'

packages: list = [
    package_name,
    mqtt_package_name,
    node_package_name
]

setup(
    name=package_name,
    version='0.0.0',
    packages=packages,
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        (os.path.join('share', package_name, 'config'), glob('config/*.yaml')),
        (os.path.join('share', package_name), glob('launch/*launch.[pxy][yma]*'))
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='reidlo',
    maintainer_email='naru5135@wavem.net',
    description='TODO: Package description',
    license='TODO: License declaration',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'rmcl_server = rmcl.main:main'
        ],
    },
)
