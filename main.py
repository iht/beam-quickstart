#  Copyright 2021 Israel Herraiz
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

from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions


def main():
    pass


def sanitizar_palabra(palabra: str) -> str:
    quitar = [",", ".", "?", "!", "-", "_"]
    for c in quitar:
        if c in palabra:
            palabra = palabra.replace(c, '')

    palabra = palabra.lower()

    palabra = palabra.replace("á", "a")
    palabra = palabra.replace("é", "e")
    palabra = palabra.replace("í", "i")
    palabra = palabra.replace("ó", "o")
    palabra = palabra.replace("ú", "u")

    return palabra


def run_pipeline(my_args, dataflow_args):
    opts = PipelineOptions(dataflow_args)

    input_file = my_args.input_file
    output_file = my_args.output_file

    with beam.pipeline.Pipeline(options=opts) as p:
        # entrada | "etiqueta" >> transform()
        entrada: PCollection[str] = p | "Leer datos" >> beam.io.ReadFromText(input_file)
        # "En un lugar de La Mancha", "lo que sea"
        # ["En", "un", "lugar", ....]
        palabras: PCollection[str] = entrada | "Partir en palabras" >> \
                                     beam.FlatMap(lambda l: l.split())
