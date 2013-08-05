#!/usr/bin/ruby
#encoding: utf-8

require 'vk-ruby'
require 'json'
require 'set'
require 'redis'
def vk
	$vk = VK::Application.new unless $vk
	$vk
end

def newsfeed
	VK::Application.new.newsfeed
end

def fetch_statuses_num(word, opts={})
	newsfeed.search( { :q => word,
					:extended=>0,
					:count => 1
					}.merge(opts)
				  )['count']
end

def fetch_statuses(word, opts={})
	newsfeed.search( { :q => word,
					:extended=>0,
					:count => 199
					}.merge(opts)
				  )
end

def htags_extract(items)
	htags = Set.new
	items.map{|i| htags|=i['text'].scan(/\#[a-zA-Zа-яА-Я0-9_]+/) }
    htags.to_a		
end

def rd
	@rd = Redis.new unless @rd
	@rd
end

def redis_store(bkey, shash)
	# w:bkey:items_type = Set.new(items)
	#
	puts '# updating data in redis for word %s'%bkey	
	shash.each do |what, items|
		sname = 'w:%s:%s'%[bkey,what]
		
		puts 'Updating set %s (size %d) with %d items'%[sname, rd.scard(sname), items.size]
		items.each{|i| rd.sadd  sname, i.to_json}
		puts 'Updated set %s. Now size %d'%[sname, rd.scard(sname)]
	end
	puts '# %d data groups stored'%shash.size
end

vk = VK::Application.new
$redis = Redis.new
words =  IO.read(ARGV[0]).split("\n")
while true

	words << '' if words.empty?
	puts 'Words num: %d'%words.size
	words.each do |word|
		tm = (Time.now.to_i - 900)
		qopts = {:start_time=>tm.to_i, :v=>'5.0', :extended=>1}
		last_opts = {}
		stats_num = fetch_statuses_num(word,qopts)
		puts 'Going to fetch %d statuses for word %s'%[stats_num, word]
		buf= Hash.new(Set.new)
		until (qopts[:start_id]||1) == 0 do
			resp = fetch_statuses(word, qopts)
			last_qopts = qopts.dup
			resp.each do |k, val|
				next unless val.is_a? Array
				buf[k] |= val
			end
			File.open('newsfeed/%s.%d'%[word,tm],'w'){|f| f.write buf.to_json}
			qopts[:offset]=resp['new_offset']
			qopts[:start_id]=resp['new_from']
		end
		qlog = {:word=>word,
			    :end_id => last_qopts[:start_id],
				:time_start => tm,
				:count => stats_num,
				:groups_num => buf['groups'].size,
				:users_num  => buf['users'].size,
				:items_num => buf['items'].size}
		File.open('searches.log', 'a'){|f| f.puts qlog.map{|k,v| '%s=%s'%[k,v]}.join("; ") }

		puts 'Done %d requests. Pushing data to redis'%buf.size
		redis_store(word, buf)
	end
end

