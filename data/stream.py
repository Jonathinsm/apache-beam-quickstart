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
import re
import json

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window

def main():
    parser = argparse.ArgumentParser(description='Fisr Pipeline')
    parser.add_argument("--input", help='Input file to process')
    parser.add_argument("--output", help='Output file processed')
    parser.add_argument("--n-words", type=int,help='Indicate the numbers of words to count')

    custom_args,beam_args = parser.parse_known_args()
    run_pipeline(custom_args,beam_args)

def word_cleaning(word):
    vowels = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
    )
    word = word.lower()
    word = re.sub(r'[^\w\s]','',word).replace(" ","")
    for a,b in vowels:
        word = word.replace(a,b)
    return word

def run_pipeline(custom_args, beam_args):
    input = custom_args.input
    output = custom_args.output
    n_words = custom_args.n_words

    opts = PipelineOptions(beam_args)

    table_schema='word:STRING,count:STRING'

    with beam.Pipeline(options=opts) as p:
        lines = (
            p 
            | "Read Message" >> beam.io.ReadFromPubSub(input).with_input_types(bytes)
            | "Dedoce" >> beam.Map(lambda x: x.decode('utf-8'))
        )

        results = (
            lines
            | "Split" >> beam.FlatMap(lambda linea: linea.split())
            | "Cleaning" >> beam.Map(word_cleaning)
            | beam.WindowInto(window.FixedWindows(15,0))
            | "Count" >> beam.combiners.Count.PerElement()
            #| "Get top list" >> beam.combiners.Top.Of(n_words,key=lambda kv: kv[1])
            #| "Get top words" >> beam.FlatMap(lambda element: element)
            | "Format" >> beam.Map(lambda element: '{"word" : "%s", "count" : "%d"}' % (element[0],element[1]))
            | "Convert to json" >> beam.Map(lambda x: json.loads(x))
            #| beam.Map(print)
        )

        results | "Write Result" >> beam.io.WriteToBigQuery(
            output,
            method='STREAMING_INSERTS',
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    main()