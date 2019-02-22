import random
import string


def gen_log(filename, lines, machine):
	"""
	Generate a log file with predefined patterns

	:param filename: log file name
	:param lines: number of lines to generate
	:param machine: the number of machine this log file belongs to
	:return: NONE
	"""
	count = {
		'only_found_on_odd': 0,
		'third_and_ninth': 0,
		'boring_message': 0,
		'rare_pattern_on_7': 0,
		'somewhat_freq': 0,
		'frequent_pattern': 0
	}
	with open(filename, 'w') as f:
		for _ in range(lines):
			# use an uniform random number between 0-1 to control
			# the frequency of each pattern
			rand = random.uniform(0, 1)

			# rare and unique patterns: only can be found on certain machines
			if rand <= 0.05:
				# pattern only can be found on odd machines
				if machine % 2 == 0:
					f.write('only_found_on_odd,' + gen_random(30) + '\n')
					count['only_found_on_odd'] += 1
				# pattern only can be found on 03 and 09
				elif machine % 3 == 0:
					f.write('third_and_ninth,' + gen_random(30) + '\n')
					count['third_and_ninth'] += 1
				# pattern only can be found on 07
				elif machine % 7 == 0:
					f.write('rare_pattern_on_7,' + gen_random(30) + '\n')
					count['rare_pattern_on_7'] += 1
				# pattern only can be found on 01, 05
				else:
					f.write('boring_message,' + gen_random(30) + '\n')
					count['boring_message'] += 1
			# somewhat frequent pattern
			elif rand <= 0.25:
				f.write('somewhat_freq,' + gen_random(30) + '\n')
				count['somewhat_freq'] += 1
			# frequent pattern
			else:
				f.write('frequent_pattern,' + gen_random(30) + '\n')
				count['frequent_pattern'] += 1

	# Save the counts of patterns on each log file for correctness check
	with open('count.txt', 'a') as f:
		f.write('Count from machine {%02d}\n' % machine)
		for key, value in count.items():
			f.write(key+':'+str(value)+'\n')
		f.write('\n')


def gen_random(length):
	"""
	Generate a random string contains lower case and upper case
	letters and 0-9 digits

	:param length: length of the random string
	:return: String
	"""
	return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


if __name__ == '__main__':
	random.seed(12450)
	for i in range(1, 11):
		logfile = 'machine.' + str(i).zfill(2) + '.log'
		gen_log(logfile, 10000, i)
