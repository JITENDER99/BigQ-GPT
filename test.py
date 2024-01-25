from main import bqsync


piyush = bqsync.Usersync()
project = "highongcp"
dataset = "Thisandthat"
table = "jp-test"
piyush.table_preview(project,dataset,table) # highongcp.Thisandthat.jp-test highongcp.Thisandthat.america_health_rankings


if __name__ == '__main__':
    print("begins")
