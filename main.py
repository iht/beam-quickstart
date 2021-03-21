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
from typing import Tuple

from apache_beam.options.pipeline_options import PipelineOptions


def tuple2str(kv):
    k, v = kv
    return "%s,%d" % (k, v)


def sanitize_word(w):
    to_remove = [',', '.', '-', ':']
    for t in to_remove:
        w = w.replace(t, '')

    w = w.lower()
    return w


def main():
    parser = argparse.ArgumentParser(description="Nuestro primer pipeline con Beam")
    parser.add_argument("--entrada", help="Fichero de entrada")
    parser.add_argument("--salida", help="Fichero de salida (cuidado, se va a sobre-escribir)")
    parser.add_argument("--n-palabras", type=int, help="Número de palabras en la salida")
    parser.add_argument("--muestra-salida", action='store_true', help="Escribe salida en pantalla")

    args, beam_args = parser.parse_known_args()
    run_pipeline(args, beam_args)


def run_pipeline(args, beam_args):
    input_file = args.entrada
    output_file = args.salida
    n_words = args.n_palabras
    show_output = args.muestra_salida

    opts = PipelineOptions(beam_args)

    with beam.Pipeline(options=opts) as p:
        lines: PCollection[str] = p | "Leer fichero entrada" >> beam.io.ReadFromText(input_file)
        words: PCollection[str] = lines | "Separa palabras" >> beam.FlatMap(lambda l: l.split())

        # Add clean words after running in Dataflow for the first time
        clean_words = words | "Sanitiza" >> beam.Map(sanitize_word)
        counted_words: PCollection[Tuple[str, int]] = clean_words \
                                                      | "Cuenta palabras" >> beam.combiners.Count.PerElement()
        top_words = counted_words | "Top %d" % n_words >> beam.combiners.Top.Of(
            n_words,
            key=lambda kv: kv[1]
        )

        formatted = top_words \
                    | "Desenvuelve lista" >> beam.FlatMap(lambda x: x) \
                    | "Formatea" >> beam.Map(tuple2str)
        formatted | beam.io.WriteToText(output_file)

        if show_output:
            formatted | beam.Map(print)


if __name__ == '__main__':
    main()
