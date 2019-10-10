DIR=/common/lib/python3
FILES=$(DIR)/sendRedis.py $(DIR)/redisPubSub.py

install: $(DIR) $(FILES)

$(DIR):
	mkdir -p $(DIR)

$(DIR)/sendRedis.py: sendRedis.py
	cp sendRedis.py $(DIR)

$(DIR)/redisPubSub.py: redisPubSub.py
	cp redisPubSub.py $(DIR)
