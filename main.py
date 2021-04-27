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
    parser = argparse.ArgumentParser(description="This is our first pipeline in Beam")
    parser.add_argument("--input", help="Input text location")
    parser.add_argument("--output", help="Output result location")
    parser.add_argument("--n-words", type=int, help="Number of words in output", default=50)

    our_args, dataflow_args = parser.parse_known_args()
    run_pipeline(our_args, dataflow_args)


# [(a,1), (b,2), (c,3)]
# a,1
# b,2
# c,3
def format_output(sorted_words):
    output_str = ""
    for pair in sorted_words:
        w, n = pair
        output_str += "%s,%d\n" % (w, n)

    return output_str


def sanitize_word(w):
    to_remove = ['.', ',', '-', ';', ':']
    for t in to_remove:
        w = w.replace(t, '')

    w = w.lower()

    return w


def run_pipeline(custom_args, runner_args):
    input_location = custom_args.input
    output_location = custom_args.output
    n_words = custom_args.n_words

    opts = PipelineOptions(runner_args)

    with beam.Pipeline(options=opts) as p:
        lines: PCollection[str] = p | "Read input text" >> beam.io.ReadFromText(input_location)
        # line.split() --> PCollection[str] => PCollection[List[str]] => PCollection[str]
        # PColl("hello all how are you doing") =>PColl(["hello", "all", ...]) => PColl("hello", "all", ...)
        words: PCollection[str] = lines | "Split into words" >> beam.FlatMap(lambda line: line.split())

        sanitized = words | "Sanitize words" >> beam.Map(sanitize_word)

        # Output: (word, N)
        counted_words = sanitized | "Count words" >> beam.combiners.Count.PerElement()

        top_n = counted_words | "Top %d" % n_words >> beam.combiners.Top.Of(
            n_words,
            key=lambda t: t[1]
        )

        output_str = top_n | "Format output" >> beam.Map(format_output)
        output_str | "Write output" >> beam.io.WriteToText(output_location)


if __name__ == '__main__':
    main()
