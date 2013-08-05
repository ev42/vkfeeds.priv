#encoding: utf-8
desc 'Start simple monitor for words: arg - words filename'
task :run do
	ARGV.shift
	puts 'ARGS: %s'%ARGV.join(', ')
	load 'simple.rb'
end


task :tags do
	require 'redis'
	require 'json'
	require 'set'
	rd = Redis.new
	items = rd.smembers('qwords').map{|w| rd.smembers('w:%s:items'%w).map{|i| JSON::parse(i)} }.flatten
	puts '#Items total %d, unique %d'%[items.size, items.uniq.size]
	tags = Hash.new(Set.new)
	items.each do |i| 
		itm = Time.at(i['date'].to_i)
		i['text'].scan(/#[0-9a-zA-Zа-яА-Я_]+/).each do |tg| 
			tags[tg.downcase]|=[{:date=>i['date'], :date_str=>Time.at(itm), :tag=>tg.downcase, :src_id => '%s.%s'%[i['owner_id'], i['id']]}]
		end
	end
	puts '# %s tags found'%tags.size
	tags_top = tags.map{|k,v| [k,v.size]}.sort{|a,b| a[1]<=>b[1]}.reverse
	puts '# top 100'
	puts tags_top.map{|t| '%s [%s]'%[t[0],t[1]]}.join("\n")
end


task :try_mapreduce do
	$LOAD_PATH << "."
	require 'vk-newsfeed.rb'
	require 'mongo'
	m=Mongo::MongoClient.new
	feed = VKFeed.new
	puts feed.query('aliens').inspect
	# - receiving some responses

	# - saving everything in mongo as a docs

	# - run map reduce with updates for tops collections
	
end


