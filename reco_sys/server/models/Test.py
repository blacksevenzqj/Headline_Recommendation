import tensorflow as tf

pets = {'pets': [2,3,0,1]}  #猫0，狗1，兔子2，猪3

column = tf.feature_column.categorical_column_with_identity(
    key='pets',
    num_buckets=4)

indicator = tf.feature_column.indicator_column(column)
tensor = tf.feature_column.input_layer(pets, [indicator])

with tf.Session() as session:
        print(session.run([tensor]))