DIR=/common/lib/python3
FILES=$(DIR)/redisPubSub.py $(DIR)/sendToRedis.py $(DIR)/getFromRedis.py

install: $(DIR) $(FILES)

$(DIR):
	mkdir -p $(DIR)

#$(DIR)/sendRedis.py: sendRedis.py
#	cp sendRedis.py $(DIR)

$(DIR)/redisPubSub.py: smax/redisPubSub.py
	cp smax/redisPubSub.py $(DIR)

$(DIR)/sendToRedis.py: smax/sendToRedis.py
	cp smax/sendToRedis.py $(DIR)

$(DIR)/getFromRedis.py: smax/getFromRedis.py
	cp smax/getFromRedis.py $(DIR)
