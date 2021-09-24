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
from apache_beam.transforms.combiners import Top

def main():
    parser = argparse.ArgumentParser(description="First Pipeline")
    parser.add_argument("--input", help="Origin file path")
    parser.add_argument("--output", help="Destination file path")
    custom_args, beam_args = parser.parse_known_args()
    run_pipeline(custom_args, beam_args)

def run_pipeline(custom_args, beam_args):
    input = custom_args.input
    output = custom_args.output
    opts = PipelineOptions(beam_args)

    with beam.Pipeline(options=opts) as p:
        lines = p | beam.io.ReadFromText(input)
        # "Habia una vez un gato" --> ["Habia","una","vez"] --> "Habia", "una", "vez" Map solo retorna cada valor de la conexión  mas la operación que se le agregue
        words = lines | beam.FlatMap(lambda l: l.split())
        counted = words | beam.combiners.Count.PerElement()
        top_words_list = counted | beam.combiners.Top.Of(500,key=lambda kv: kv[1]) #Devuele Una lista de Tuplas
        top_words = top_words_list | beam.FlatMap(lambda x: x) #Poner a recordar comcepto flat map
        formateado = top_words | beam.Map(lambda kv: "%s,%d" % (kv[0], kv[1])) #Le doy formato a la salida
        #formateado | beam.Map(print)
        #Listo imprimir salida
        formateado | beam.io.WriteToText(output)
    

if __name__ == "__main__":
    main()