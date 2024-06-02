DROP KEYSPACE IF EXISTS shop_db;
DROP KEYSPACE IF EXISTS micro_blog_db;
DROP KEYSPACE IF EXISTS bank_acc_db;

CREATE KEYSPACE IF NOT EXISTS shop_db WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
CREATE KEYSPACE IF NOT EXISTS micro_blog_db WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
CREATE KEYSPACE IF NOT EXISTS bank_acc_db WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };