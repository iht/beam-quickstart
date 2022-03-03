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
from typing import List, Tuple

import apache_beam as beam
import argparse

from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--fichero-entrada", required=True)
    parser.add_argument("--fichero-salida", required=True)
    parser.add_argument("--num-palabras", required=True)

    known_args, dataflow_args = parser.parse_known_args()
    run_pipeline(known_args, dataflow_args)


def sanitizar_palabra(palabra: str) -> str:
    quitar = [",", ".", "?", "!", "-", "_", "¿", "¡"]
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


def lista_a_csv(l: List[Tuple[str, int]]) -> str:
    csv = ""
    for e in l:
        csv += f"{e[0]},{e[1]}\n"

    return csv


def run_pipeline(my_args, dataflow_args):
    opts = PipelineOptions(dataflow_args)

    input_file = my_args.fichero_entrada
    output_file = my_args.fichero_salida
    num_palabras = int(my_args.num_palabras)

    with beam.pipeline.Pipeline(options=opts) as p:
        # entrada | "etiqueta" >> transform()
        entrada: PCollection[str] = p | "Leer datos" >> beam.io.ReadFromText(input_file)
        # "En un lugar de La Mancha", "lo que sea"
        # ["En", "un", "lugar", ....]
        palabras: PCollection[str] = entrada | "Partir en palabras" >> \
                                     beam.FlatMap(lambda l: l.split())

        # ["en", "un", "lugar", ....]
        palabras_limpias = palabras | "Limpiar palabras" >> beam.Map(sanitizar_palabra)

        # ("en", 7), ("sancho", 20), ("dulcinea", ???), ("mancha", 14)
        contadas = palabras_limpias | "Contar" >> beam.combiners.Count.PerElement()

        top_palabras = contadas | "Top palabras" >> \
                       beam.combiners.Top.Of(num_palabras, key=lambda t: t[1])

        lineas_csv: PCollection[str] = top_palabras | "A CSV" >> beam.Map(lista_a_csv)
        lineas_csv | "Escribir salida" >> beam.io.WriteToText(output_file)


if __name__ == '__main__':
    main()
