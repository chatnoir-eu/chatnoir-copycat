#!/usr/bin/env python3

def transform(json_line):
    import json
    json_line = json.loads(json_line)
    json_line['content'] = json_line['contents']
    del json_line['contents']
    del json_line['topic']
    del json_line['chatNoirUrl']

    return json.dumps(json_line)


def parse_args():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--input')
    parser.add_argument('--output')

    return parser.parse_args()


if __name__ == '__main__':
    from tqdm import tqdm

    args = parse_args()

    with open(args.input) as input_file, open(args.output, 'w') as output_file:
        for l in tqdm(input_file):
            output_file.write(transform(l) + '\n')

