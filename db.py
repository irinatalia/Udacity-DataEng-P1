filepath = "data/song_data"
song_files = get_files(filepath)
print("file {}: {}".format(0, song_files[0]))
#df = song_files[0]
#for i, datafile in enumerate(song_files, 1):
#    print("file {}: {}".format(i, datafile))
with open(song_files[0], 'r') as myfile:
    data=myfile.read()
print(data)

df = pd.read_json(data)
df.head()


filepath = "data/song_data"
song_files = get_files(filepath)

with open(song_files[0]) as f:
  dt = json.load(f)

#print(dt['song_id'])
#print(dt)

df = pd.DataFrame(dt, index=['0'])
df.head()
#print(df.columns)

song_data = [df['song_id'][0], df['title'][0]]
song_data
