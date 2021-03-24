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
from typing import Tuple

import apache_beam as beam
import argparse

from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions


def main():
  parser = argparse.ArgumentParser(description="Nuestro primer pipeline")
  parser.add_argument("--entrada", help="Fichero de entrada")
  parser.add_argument("--salida", help="Fichero de salida")

  our_args, beam_args = parser.parse_known_args()
  run_pipeline(our_args, beam_args)


def run_pipeline(custom_args, beam_args):
  entrada = custom_args.entrada
  salida = custom_args.salida

  opts = PipelineOptions(beam_args)

  with beam.Pipeline(options=opts) as p:
    lineas: PCollection[str] = p | beam.io.ReadFromText(entrada)
    # "En un lugar de La Mancha" --> ["En", "un", ...], [...], [...] --> "En", "un", "lugar", ....
    palabras = lineas | beam.FlatMap(lambda l: l.split())
    contadas: PCollection[Tuple[str, int]] = palabras | beam.combiners.Count.PerElement()
    # "En" -> ("En", 17)
    # "un" -> ("un", 28)
    palabras_top_lista = contadas | beam.combiners.Top.Of(5, key=lambda kv: kv[1])
    palabras_top = palabras_top_lista | beam.FlatMap(lambda x: x)
    formateado: PCollection[str] = palabras_top | beam.Map(lambda kv: "%s,%d" % (kv[0], kv[1]))
    formateado | beam.io.WriteToText(salida)



if __name__ == '__main__':
  main()