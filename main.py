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

from apache_beam.options.pipeline_options import PipelineOptions

def word_cleaning(word):
    vocal_repl = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
    )
    word = word.lower()
    word = re.sub(r'[^\w\s]','',word).replace(" ","")
    for a,b in vocal_repl:
        word = word.replace(a,b)
    return word

def main():
    parser = argparse.ArgumentParser(description="First Pipeline")
    parser.add_argument("--in-file",help="File to count words")
    parser.add_argument("--out-file", help="Output file of results")
    parser.add_argument("--n-count", type=int, help="Indicate the number of words to return")

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args,beam_args)

def run_pipeline(custom_args, beam_args):
    inf = custom_args.in_file
    outf = custom_args.out_file
    n_count = custom_args.n_count

    opts = PipelineOptions(beam_args)

    with beam.Pipeline(options=opts) as p:
        lines = p | "Read File" >> beam.io.ReadFromText(inf)
        words = lines | "Split Words" >> beam.FlatMap(lambda l: l.split())
        #words | beam.Map(print)
        cleaned_words = words | "Clean Words" >> beam.Map(word_cleaning)
        #cleaned_words | beam.Map(print)
        counted = cleaned_words | "Count Words" >> beam.combiners.Count.PerElement()
        list_top_words = counted | "List Words"  >> beam.combiners.Top.Of(n_count, key=lambda kv: kv[1])
        top_words = list_top_words | "Get Top Words" >> beam.FlatMap(lambda x: x)
        formated = top_words | "Format" >> beam.Map(lambda kv: "%s,%d" % (kv[0], kv[1]))
        formated | "Write Result" >> beam.io.WriteToText(outf)

if __name__ == '__main__':
    main()