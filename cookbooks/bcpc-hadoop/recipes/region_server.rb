include_recipe 'bcpc-hadoop::hbase_config'

%w{hbase-regionserver libsnappy1}.each do |pkg|
  package pkg do
    action :install
  end
end

directory "/usr/lib/hbase/lib/native/Linux-amd64-64/" do
  recursive true
  action :create
end

link "/usr/lib/hbase/lib/native/Linux-amd64-64/libsnappy.so.1" do
  to "/usr/lib/libsnappy.so.1"
end

#
# When there is a need to restart HBase region server, a lock need to be taken so that the restart is sequenced preventing all RSs being down at the
# same time. If there is a failure in acquiring a lock with in a certian period, the restart is scheduled for the next run on chef-client on the node.
# To determine whether the prev restart failed is the node attribute node[:bcpc][:hadoop][:hbase_regionserver][:restart_failed] is set to true
# This ruby block is to check whether this node attribute is set to true and if it is set then gets the RS restart process in motion.
#
ruby_block "handle_prev_region_server_restart_failure" do
  block do
    Chef::Log.info "Need to restart RS server since it failed during the previous run. Another node's RS server restart process failure is a possible reason"
  end
  action :create
  only_if { node[:bcpc][:hadoop][:hbase_regionserver][:restart_failed] }
end

#
# Since string with all the zookeeper nodes is used multiple times this variable is populated once and reused reducing calls to Chef server
#
zk_hosts = (get_node_attributes(MGMT_IP_ATTR_SRCH_KEYS,"zookeeper_server","bcpc-hadoop").map{|zkhost| "#{zkhost['mgmt_ip']}:#{node[:bcpc][:hadoop][:zookeeper][:port]}"}).join(",")

#
# znode is used as the locking mechnism to control restart of services. The following code is to build the path
# to create the znode before initiating the restart of HBase region server service 
#
if node[:bcpc][:hadoop][:restart_lock].attribute?(:root) 
  lock_znode_path = format_restart_lock_path(node[:bcpc][:hadoop][:restart_lock][:root], "hbase-regionserver")
else
  lock_znode_path = format_restart_lock_path("/", "hbase-regionserver")
end   

#
# All HBase region server restart situations like changes in config files or restart due to previous failures invokes this ruby_block
# This ruby block tries to acquire a lock and if not able to acquire the lock, sets the restart_failed node attribute to true
#
ruby_block "acquire_lock_to_restart_rs" do
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
          Chef::Log.info("Couldn't acquire lock to restart HBase RS with in the #{node[:bcpc][:hadoop][:restart_lock_acquire][:max_tries] * node[:bcpc][:hadoop][:restart_lock_acquire][:sleep_time]} secs.")
          Chef::Log.info("Node #{get_restart_lock_holder(lock_znode_path, zk_hosts)} may have died during HBase RS server restart.")
          node.set[:bcpc][:hadoop][:zookeeper_server][:restart_failed] = true
          node.save
          break
        end
        sleep(node[:bcpc][:hadoop][:restart_lock_acquire][:sleep_time])
      end
    end
  end
  action :nothing
  subscribes :create, "template[/etc/hbase/conf/hbase-site.xml]", :immediate
  subscribes :create, "template[/etc/hbase/conf/hbase-policy.xml]", :immediate
  subscribes :create, "template[/etc/hbase/conf/hbase-env.sh]", :immediate
  subscribes :create, "ruby_block[handle_prev_region_server_restart_failure]", :immediate
  only_if { node[:bcpc][:hadoop][:hbase_10070][:enabled] }
end

#
# Temporary variable to store the original state of HBase balancer so that the state can be restored if disables for RS restart 
#
hbase_balancer_state = nil

#
# If lock to restart RS server is acquired by the node, this ruby_block executes which is primarily used to notify the RS server service to restart
#
ruby_block "coordinate_rs_server_restart" do
  block do
    Chef::Log.info("HBase region server will be restarted in node #{node[:fqdn]}")
    hbase_balancer_state =`echo 'balance_switch false' | hbase shell | tail -3 | head -1`
    Chef::Log.info("HBase balancer was disabled. Originally balancer was set to #{hbase_balancer_state}")
  end
  action :create
  only_if { my_restart_lock?(lock_znode_path, zk_hosts, node[:fqdn]) }
end

service "hbase-regionserver" do
  supports :status => true, :restart => true, :reload => false
  action [:enable, :start]
  subscribes :restart, "ruby_block[coordinate_rs_server_restart]", :immediate
end

#
# Once the RS server service restart is complete, the following block releases the lock if the node executing is the one which holds the lock 
#
ruby_block "release_rs_server_restart_lock" do
  block do
    Chef::Log.info("#{node[:hostname]}: Releasing lock at #{lock_znode_path}")
    balancer_state = `echo 'balance_switch #{hbase_balancer_state}' | hbase shell | tail -3 | head -1`
    Chef::Log.info("HBase balancer is restored to orginial state : #{hbase_balancer_state}")
    lock_rel = rel_restart_lock(lock_znode_path, zk_hosts, node[:fqdn])
    if lock_rel      
      node.set[:bcpc][:hadoop][:hbase_regionserver][:restart_failed] = false
      node.save
    end
  end
  action :create
  only_if { my_restart_lock?(lock_znode_path, zk_hosts, node[:fqdn]) }
end

