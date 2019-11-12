DIR=/common/lib/python3
FILES=$(DIR)/sendRedis.py $(DIR)/redisPubSub.py $(DIR)/sendToRedis.py \
    $(DIR)/getFromRedis.py

install: $(DIR) $(FILES)

$(DIR):
	mkdir -p $(DIR)

$(DIR)/sendRedis.py: sendRedis.py
	cp sendRedis.py $(DIR)

$(DIR)/redisPubSub.py: redisPubSub.py
	cp redisPubSub.py $(DIR)

$(DIR)/sendToRedis.py: sendToRedis.py
	cp sendToRedis.py $(DIR)

$(DIR)/getFromRedis.py: getFromRedis.py
	cp getFromRedis.py $(DIR)
