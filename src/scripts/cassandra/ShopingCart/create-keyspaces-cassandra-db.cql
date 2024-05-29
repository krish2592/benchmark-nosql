CREATE KEYSPACE IF NOT EXISTS shop_db WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
CREATE KEYSPACE IF NOT EXISTS micro_blog_db WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
CREATE KEYSPACE IF NOT EXISTS bank_acc_db WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };