#!/usr/bin/ruby
#encoding: utf-8

require 'vk-ruby'
require 'json'
require 'set'
require 'redis'
module VK
	LOGGER = nil
	ATTEMPTS = 2
end

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

def redis_store(bkey, shash)
	rd = Redis.new
	# w:bkey:items_type = Set.new(items)
	#
	puts '# updating data in redis for word %s'%bkey	
	shash.each do |what, items|
		sname = 'w:%s:%s'%[bkey,what]
		
		puts 'Updating set %s (size %d) with %d items'%[sname, rd.scard(sname), items.size]
		items.each{|i| rd.sadd  sname, i.to_json}
		puts 'Updated set %s. Now size %d'%[sname, rd.scard(sname)]
	end
	shash['items'].each do |item|
		rd.sadd 'vknews_all', [item['date'],item['text'].downcase].to_json
	end
	puts '# %d data groups stored'%shash.size
	rd.quit
end

at_exit {
	puts 'Killing childs %s'%$childs.join(' ')
	$childs.each{|cpid| Process.kill(9,cpid) }
	Process.waitall
	puts 'Killed childs %s'%$childs.join(' ')
}

$childs = []

threads_num = ARGV[1].to_i||32
start_time = Time.now
words_dict =  IO.read(ARGV[0]).split("\n")
rd = Redis.new
words_dict.each_slice((words_dict.size.to_f/threads_num).ceil) do |words|
	$childs << fork do 
		at_exit{}
		vk = VK::Application.new 
		$redis = Redis.new
		#puts 'Words num: %d'%words.size
		while true
			words_dict.each do |word|
				words << '' if words.empty?
				tm = (Time.now.to_i - 900)
				qopts = {:start_time=>tm.to_i, :v=>'5.0', :extended=>1}
				buf= Hash.new(Set.new)
				until (qopts[:start_id]||1) == 0 do
					resp = fetch_statuses(word, qopts)
					last_qopts = qopts.dup
					resp.each do |k, val|
						next unless val.is_a? Array
						buf[k] |= val
					end
					qopts[:offset]=resp['new_offset']
					qopts[:start_id]=resp['new_from']
				end
				puts '#done word "%s" requests. Pushing data to redis'%word
				redis_store(word, buf)
			end
		end
	end
	sleep 15
end


Process.waitall
puts 'Finished in %s seconds'%(Time.now-start_time).to_i
