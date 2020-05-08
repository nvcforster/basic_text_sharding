# This script takes in a single text file with one article per line
# and outputs text shards with one setence per line and one blank line between articles

# Be sure to create shards directory before running (keeping the script simple)

import nltk
nltk.download('punkt')

# User-defined parameters
input_text_file = 'sample_data.txt'  # One article per line
intermediate_text_file = 'data_segmented.txt'  # One sentence per line
shard_path = 'shards'  # subdirectory to put shards in
n_shards = 1024  # Pick too few and create_pretraining_data.py takes more memory and time (higher is faster but limits randomization of dataset)
#n_shards = 4  # for testing with sample file

def segment_article(article):
  return nltk.tokenize.sent_tokenize(article)  # returns a list of sentences

# Create list of shards
shard_names = []
for i in range(n_shards):
  shard_names.append(shard_path + '/' + 'text_shard_' + str(i) + '.txt')

shard_handles = []
for shard_name in shard_names:
  shard_handles.append(open(shard_name, 'w'))

article_counter = 0
with open(input_text_file, mode='r', encoding='utf-8', newline='\n') as input_file:
  for line in input_file:
    if line.rstrip() == '':
      continue
    sentences = segment_article(line)
    
    # Write to shards in round robin order
    for output_line in sentences:
      if output_line.rstrip() == '':
        continue
      shard_handles[article_counter % n_shards].write(output_line.rstrip())  # just in case there is any trailing whitespace aside from a single newline (may not be necessary)
      shard_handles[article_counter % n_shards].write('\n')
    
    shard_handles[article_counter % n_shards].write('\n')
    article_counter += 1

for shard_handle in shard_handles:
  shard_handle.close()
