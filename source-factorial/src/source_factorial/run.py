# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceFactorial

def run():
    apikey = "08c46a26d9f59867002d624eb08e7740e81088fe0c881e5670af62f87b8459d8" 
    source = SourceFactorial(apikey)
    launch(source, sys.argv[1:])