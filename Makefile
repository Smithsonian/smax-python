DIR=/common/lib/python3
FILES=$(DIR)/sendRedis.py $(DIR)/redisPubSub.py $(DIR)/sendToRedis.py \
    $(DIR)/getFromRedis.py

install: $(DIR) $(FILES)

$(DIR):
	mkdir -p $(DIR)

$(DIR)/sendRedis.py: smax/sendRedis.py
	cp sendRedis.py $(DIR)

$(DIR)/redisPubSub.py: smax/redisPubSub.py
	cp redisPubSub.py $(DIR)

$(DIR)/sendToRedis.py: smax/sendToRedis.py
	cp sendToRedis.py $(DIR)

$(DIR)/getFromRedis.py: smax/getFromRedis.py
	cp getFromRedis.py $(DIR)
