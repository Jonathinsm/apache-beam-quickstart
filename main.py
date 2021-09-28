#  Copyright 2021 Jonathan Simanca
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import apache_beam as beam
import argparse

from apache_beam.options.pipeline_options import PipelineOptions


def main():
    parser = argparse.ArgumentParser(description='Fisr Pipeline')
    parser.add_argument("--input", help='Input file to process')
    parser.add_argument("--output", help='Output file processed')
    custom_args,beam_args = parser.parse_known_args()
    run_pipeline(custom_args,beam_args)


def run_pipeline(custom_args, beam_args):
    input = custom_args.input
    output = custom_args.output

    opts = PipelineOptions(beam_args)

    with beam.Pipeline(options=opts) as p:
        pass

if __name__ == '__main__':
    main()