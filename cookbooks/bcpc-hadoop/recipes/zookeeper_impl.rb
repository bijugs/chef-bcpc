
include_recipe 'dpkg_autostart'
include_recipe 'bcpc-hadoop::zookeeper_config'
dpkg_autostart "zookeeper-server" do
  allow false
end

package  "zookeeper-server" do
  action :upgrade
  notifies :create, "template[/tmp/zkServer.sh]", :immediately
  notifies :create, "ruby_block[Compare_zookeeper_server_start_shell_script]", :immediately
end

template "/tmp/zkServer.sh" do
  source "hdp_zkServer.sh.orig.erb"
  mode 0644
end

ruby_block "Compare_zookeeper_server_start_shell_script" do
  block do
    require "digest"
    orig_checksum=Digest::MD5.hexdigest(File.read("/tmp/zkServer.sh"))
    new_checksum=Digest::MD5.hexdigest(File.read("/usr/lib/zookeeper/bin/zkServer.sh"))
    if orig_checksum != new_checksum
      Chef::Application.fatal!("zookeeper-server:New version of zkServer.sh need to be created and used")
    end
  end
  action :nothing
end

template "/etc/init.d/zookeeper-server" do
  source "hdp_zookeeper-server.start.erb"
  mode 0655
end

directory node[:bcpc][:hadoop][:zookeeper][:data_dir] do
  recursive true
  owner node[:bcpc][:hadoop][:zookeeper][:owner]
  group node[:bcpc][:hadoop][:zookeeper][:group]
  mode 0755
end

template "/etc/default/zookeeper-server" do
  source "hdp_zookeeper-server.default.erb"
  mode 0644
  variables(:zk_jmx_port => node[:bcpc][:hadoop][:zookeeper][:jmx][:port])
end

template "/usr/lib/zookeeper/bin/zkServer.sh" do
  source "hdp_zkServer.sh.erb"
end

bash "init-zookeeper" do
  code "service zookeeper-server init --myid=#{node[:bcpc][:node_number]}"
  not_if { ::File.exists?("#{node[:bcpc][:hadoop][:zookeeper][:data_dir]}/myid") }
end

file "#{node[:bcpc][:hadoop][:zookeeper][:data_dir]}/myid" do
  content node[:bcpc][:node_number]
  owner node[:bcpc][:hadoop][:zookeeper][:owner]
  group node[:bcpc][:hadoop][:zookeeper][:group]
  mode 0644
end

#
# When there is a need to restart ZooKeeper server, a lock need to be taken so that the restart is sequenced preventing all ZKs being down at the sametime
# If there is a failure in acquiring a lock with in a certian period, the restart is scheduled for the next run on chef-client on the node.
# To determine whether the prev restart failed is the node attribute node[:bcpc][:hadoop][:zookeeper_server][:restart_failed] is set to true
# This ruby block is to check whether this node attribute is set to true and if it is set then gets the ZK restart process in motion.
#
ruby_block "handle_prev_zk_server_restart_failure" do
  block do
    Chef::Log.info "Need to restart ZK server since it failed during the previous run. Another node's ZK server restart process failure is a possible reason"
  end
  action :create
  only_if { node[:bcpc][:hadoop][:zookeeper_server][:restart_failed] }
end

#
# Since string with all the zookeeper nodes is used multiple times this variable is populated once and reused reducing calls to Chef server
#
zk_hosts = (get_node_attributes(MGMT_IP_ATTR_SRCH_KEYS,"zookeeper_server","bcpc-hadoop").map{|zkhost| "#{zkhost['mgmt_ip']}:#{node[:bcpc][:hadoop][:zookeeper][:port]}"}).join(",")
#
# znode is used as the locking mechnism to control restart of services. The following code is to build the path
# to create the znode before initiating the restart of ZooKeeper server 
#
if node[:bcpc][:hadoop][:restart_lock].attribute?(:root) 
  lock_znode_path = format_restart_lock_path(node[:bcpc][:hadoop][:restart_lock][:root], "zookeeper-server")
else
  lock_znode_path = format_restart_lock_path("/", "zookeeper-server")
end   
#
# All ZooKeeper restart situations like changes in config files or restart due to previous failures invokes this ruby_block
# This ruby block tries to acquire a lock and if not able to acquire the lock, sets the restart_failed node attribute to true
#
ruby_block "acquire_lock_to_restart_zk_server" do
  block do
    tries = 0
    Chef::Log.info("#{node[:hostname]}: Acquring lock at #{lock_znode_path}")
    while true 
      lock = acquire_restart_lock(lock_znode_path, zk_hosts, node[:fqdn])
      if lock
        break
      else
        tries += 1
        if tries >= node[:bcpc][:hadoop][:restart_lock_acquire][:max_tries]
          Chef::Log.info("Couldn't acquire lock to restart ZK server with in the #{node[:bcpc][:hadoop][:restart_lock_acquire][:max_tries] * node[:bcpc][:hadoop][:restart_lock_acquire][:sleep_time]} secs.")
          Chef::Log.info("Node #{get_restart_lock_holder(lock_znode_path, zk_hosts)} may have died during zk server restart.")
          node.set[:bcpc][:hadoop][:zookeeper_server][:restart_failed] = true
          node.save
          break
        end
        sleep(node[:bcpc][:hadoop][:restart_lock_acquire][:sleep_time])
      end
    end
  end
  action :nothing
  subscribes :create, "template[/etc/zookeeper/conf/zoo.cfg]", :immediate
  subscribes :create, "template[/usr/lib/zookeeper/bin/zkServer.sh]", :immediate
  subscribes :create, "template[/etc/default/zookeeper-server]", :immediate
  subscribes :create, "file[#{node[:bcpc][:hadoop][:zookeeper][:data_dir]}/myid]", :immediate
  subscribes :create, "ruby_block[handle_prev_zk_server_restart_failure]", :immediate
end

#
# If lock to restart ZK server is acquired by the node, this ruby_block executes which is primarily used to notify the ZK server service to restart
#
ruby_block "coordinate_zk_server_restart" do
  block do
    Chef::Log.info("ZooKeeper server will be restarted in node #{node[:fqdn]}")
  end
  action :create
  only_if { my_restart_lock?(lock_znode_path, zk_hosts, node[:fqdn]) }
end

service "zookeeper-server" do
  supports :status => true, :restart => true, :reload => false
  action [:enable, :start]
  subscribes :restart, "ruby_block[coordinate_zk_server_restart]", :immediate
end

#
# Once the ZK server service restart is complete, the following block releases the lock if the node executing is the one which holds the lock 
#
ruby_block "release_zk_server_restart_lock" do
  block do
    Chef::Log.info("#{node[:hostname]}: Releasing lock at #{lock_znode_path}")
    lock_rel = rel_restart_lock(lock_znode_path, zk_hosts, node[:fqdn])
    if lock_rel
      node.set[:bcpc][:hadoop][:zookeeper_server][:restart_failed] = false
      node.save
    end
  end
  action :create
  only_if { my_restart_lock?(lock_znode_path, zk_hosts, node[:fqdn]) }
end
