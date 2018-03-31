#!/usr/bin/env python
import click
import re

PAGE_WIDTH = 80

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
            # curr_book, curr_offset = re.sub('[<>]', '', c).split(',')[:2]
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


def run_search(search_phrase, show_single_word_results=False):

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
                    print ' {}'.format(k[:-4].upper())
                    print "\tAt locations: {}".format(sorted([int(x) for x in v]))
                print '-'*PAGE_WIDTH + '\n'

            books_set_list.append(set(words_map[word].keys()))

    phrase_books_set = reduce(lambda x, y: x & y, books_set_list)
    print "Books that contain each of the words in the phrase:"
    for s in list(phrase_books_set):
        # Build the occurrence locations for these phrase words in this book
        occurrence_locations = []
        for w in valid_search_phrase_words:
            occurrence_locations.extend([int(x) for x in words_map[w][s]])
        occurrence_locations.sort()

        print '\n {}'.format(s[:-4].upper())
        print ' Locations: {}'.format(occurrence_locations)

    print '-'*PAGE_WIDTH + '\n'


@click.command()
@click.option('--in_file', default='test.txt', help='Input file for reverse index.')
@click.option('--search_phrase', default=None, help='Phrase to search.')
@click.option('--show_words/--no_show_words', default=False, help='Show the list of words in the reverse index.')
@click.option('--show_single_word_results/--no_show_single_word_results', default=False,
              help='Show occurrence results for the single words in a multi-word phrase.')
# @click.argument('search_phrase')
def search_word_map(in_file, search_phrase, show_words, show_single_word_results):
    # click.echo('Starting...')

    print "in_file = {}\n".format(in_file)
    # print "search_phrase = {}".format(search_phrase)

    build_words_map(in_file)

    if search_phrase is not None:
        run_search(search_phrase, show_single_word_results)

    if show_words:
        print "\n\n" + '-'*PAGE_WIDTH
        print "List of words:"
        print words_map.keys()


if __name__ == '__main__':
    search_word_map()
