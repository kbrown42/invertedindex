#!/usr/bin/env python
import click
import re

words_count = {}  # Dictionary for word counts


def add_words(d_in, line_in):
    """
    Accumulate word counts in the dictionary
    :param d_in: Dictionary of word counts
    :param line_in: word list from file line
    :return: (None. Dictionary updated)
    """
    for l in line_in:
        curr_str = re.sub(r'\W+', '', l).lower()
        curr_cnt = 0 if d_in.get(curr_str) is None else d_in.get(curr_str)
        d_in.update({curr_str: curr_cnt+1})


def file_to_list(in_file):
    """
    Convert a file to a list
    :param in_file: String. File name.
    :return: 2D list of words and file lines
    """
    lines = []
    f = open(in_file)
    for l in f:
        lines.append(l.split())
    return lines


@click.command()
@click.option('--thresh_pct', default=0.005, help='Threshold percent for stop words.')
@click.argument('in_file')
def gen_sw_list(in_file, thresh_pct):
    """
    Simple program to generate a stop words list.
    :param in_file: Name of file to read
    :param thresh_pct: Threshold percent for stop words
    :return: (None) Output files generated
    """
    click.echo('Starting...')
    
    file_lines = file_to_list(in_file)
    for line in file_lines:
        add_words(words_count, line)

    f_wordcount = open(in_file[:-4] + '_wordcount.txt', 'w')
    f_stopwords = open(in_file[:-4] + '_stop_words.txt', 'w')

    # Sort alphabetically, then by count
    sorted_words_list = sorted(words_count.items())
    sorted_words_list.sort(reverse=True, key=lambda x: x[1])

    total_words = float(sum(l[-1] for l in sorted_words_list))
    stop_word_count = 0

    # Write the words list
    for x, y in sorted_words_list:
        s_out = "{} {}".format(str(x).ljust(15), y)
        print s_out
        f_wordcount.write(s_out + '\n')

        if y/total_words >= thresh_pct:
            f_stopwords.write(x + '\n')
            stop_word_count += 1

    f_wordcount.close()
    f_stopwords.close()

    print "Total word count = {}".format(total_words)
    print "Number of Stopwords = {}".format(stop_word_count)


if __name__ == '__main__':
    gen_sw_list()
