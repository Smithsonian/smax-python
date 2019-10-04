DIR=/common/lib/python3
FILES=$(DIR)/sendRedis.py

install: $(DIR) $(FILES)

$(DIR):
	mkdir -p $(DIR)

$(DIR)/sendRedis.py: sendRedis.py
	cp sendRedis.py $(DIR)
