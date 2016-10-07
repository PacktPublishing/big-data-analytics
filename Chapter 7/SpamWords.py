# Spam checker
spam_words = ("No investment", "Why pay more?", "You are a winner!", "Free quote")

import sys
for line in sys.stdin:
	if sparm_words in line:
		print "Spam Found"
	else:
		process_lines()
