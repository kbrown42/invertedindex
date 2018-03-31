#!/usr/bin/env python
import click
import re
import numpy as np

PAGE_WIDTH = 80
np.set_printoptions(edgeitems=3)
np.core.arrayprint._line_width = PAGE_WIDTH

words_map = {}  # Dictionary of word occurances


def build_words_map(file_name):
    """
    Build the word_map.
    It's a dictionary of words with dictionaries of books with lists of occurrence locations
    :param file_name:
    :return: None. words_map is built
    """
    f = open(file_name)

    # Foreach line in the file
    for l in f:
        # Split the line by tabs, the word is index 0,
        # The remaining indices are occurrences of the word in the format '<book_title, offset_value>'
        curr_data = l.split('\t')

        curr_word = curr_data[0].lower()
        words_map[curr_word] = {}  # Add the new word to the dictionary with an empty dictionary of occurrences
        # Foreach word occurrence
        for c in curr_data[1:]:
            # Split the occurrence into [book_title, occurrence_offset]
            curr_occurrence = re.sub('[<>]', '', c).split(',')
            if len(curr_occurrence) != 2:
                continue

            curr_book, curr_offset = curr_occurrence

            # Add the occurrences of this word's entries to the book's dictionary

            # If the current book is not is this word's dictionary
            if curr_book not in words_map[curr_word]:
                words_map[curr_word][curr_book] = []  # add an empty list for its occurrence offsets

            # Add this occurrence offset to this word's book
            words_map[curr_word][curr_book].append(curr_offset)


def most_common(list_in):
    common_elm = max(set(list_in), key=list_in.count)
    elm_count = list_in.count(common_elm)
    return common_elm, elm_count


def run_search(search_phrase, books_location, show_single_word_results=False):

    search_phrase = re.sub('[,.!?]', '', search_phrase)
    words = [x.lower() for x in search_phrase.split(' ')]
    books_set_list = []
    valid_search_phrase_words = []

    for word in words:

        if word not in words_map:
            print "Word not found: {}\n".format(word)

        else:
            valid_search_phrase_words.append(word)

            if show_single_word_results:
                print '-'*PAGE_WIDTH
                print "\'{}\' appears in:".format(word.upper())
                for k, v in sorted(words_map[word].items()):
                    print '{}, at locations:'.format(k[:-4].upper())
                    col_formatted_locations = np.array(sorted([int(x) for x in v]))
                    print "{}\n".format(col_formatted_locations)
                print '-'*PAGE_WIDTH + '\n'

            books_set_list.append(set(words_map[word].keys()))

    phrase_books_set = reduce(lambda x, y: x & y, books_set_list)

    book_top_occurrence_name = ''
    book_top_occurrence_count = 0
    book_top_occurrence_offset = 0

    print "Books that contain each of the words in the phrase:"
    for s in list(phrase_books_set):
        # Build the occurrence locations for these phrase words in this book
        occurrence_locations = []
        for w in valid_search_phrase_words:
            occurrence_locations.extend([int(x) for x in words_map[w][s]])
        occurrence_locations.sort()
        curr_most_common_offset, curr_most_common_occurrence = most_common(occurrence_locations)
        if curr_most_common_occurrence > book_top_occurrence_count:
            book_top_occurrence_name = s
            book_top_occurrence_count = curr_most_common_occurrence
            book_top_occurrence_offset = curr_most_common_offset

        print '\n {}'.format(s[:-4].upper())
        col_formatted_locations = np.array(occurrence_locations)
        print ' Locations:\n{}'.format(col_formatted_locations)

    # Show winning excerpt
    try:
        file_location = books_location + book_top_occurrence_name
        f = open(file_location)
    except IOError:
        print "File not found: {}".format(file_location)
        print "Try setting the --book_locations parameter or adding the book files."
        return

    line_num = 0
    # Figure out the proper line number
    while f.tell() < book_top_occurrence_offset:
        l = f.readline()
        line_num += 1

    excerpt = f.readline()

    print '-' * PAGE_WIDTH + '\n'
    print 'WINNER: Search phrase in {}, line # {}:'.format(book_top_occurrence_name, line_num)
    print "EXCERPT:"
    print '\"' + excerpt.strip('\n') + '\"'

    print '-'*PAGE_WIDTH + '\n'


@click.command()
@click.option('--in_file', default='test.txt', help='Input file for reverse index.')
@click.option('--books_location', default='../data/', help='Directory location for books.')
@click.option('--search_phrase', default=None, help='Phrase to search.')
@click.option('--show_words/--no_show_words', default=False, help='Show the list of words in the reverse index.')
@click.option('--show_single_word_results/--no_show_single_word_results', default=False,
              help='Show occurrence results for the single words in a multi-word phrase.')
# @click.argument('search_phrase')
def search_word_map(in_file, books_location, search_phrase, show_words, show_single_word_results):
    # click.echo('Starting...')

    print "in_file = {}\n".format(in_file)
    # print "search_phrase = {}".format(search_phrase)

    build_words_map(in_file)

    if search_phrase is not None:
        run_search(search_phrase, books_location, show_single_word_results)

    if show_words:
        print "\n\n" + '-'*PAGE_WIDTH
        print "List of words:"
        print words_map.keys()


if __name__ == '__main__':
    search_word_map()
