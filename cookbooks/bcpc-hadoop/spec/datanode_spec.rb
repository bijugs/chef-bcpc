require "chefspec"
require "chef/application"

describe 'bcpc-hadoop::datanode' do
  let(:chef_run) do
    ChefSpec::Runner.new  do |node|
      node.set[:bcpc][:hadoop][:mounts] = ["a","b","c"]
      node.set[:bcpc][:hadoop][:disks] = ["a","b","c"]
      node.set[:bcpc][:hadoop][:hdfs][:failed_volumes_tolerated] = 2 
    end.converge(described_recipe)
  end

  it 'includes recipe bcpc-hadoop::hadoop_config' do
    expect(chef_run).to include_recipe('bcpc-hadoop::datanode')
  end

  it 'should install datanode packages' do
    %w{hadoop-yarn-nodemanager
       hadoop-hdfs-datanode
       hadoop-mapreduce
       hadoop-client
       sqoop
       lzop
       hadoop-lzo}.each do |pkg|
         expect(chef_run).to upgrade_package(pkg)
       end
  end

  it 'should create config file container-executor.cfg' do
    expect(chef_run).to create_template('/etc/hadoop/conf/container-executor.cfg').with(owner: 'root', group: 'yarn', mode: '0400') 
  end

  it 'notifies to run verify-container-executor' do
    resource = chef_run.template('/etc/hadoop/conf/container-executor.cfg')
    expect(resource).to notify('bash[verify-container-executor]').to(:run).immediately
  end

  it 'should take no action on bash[verify-container-executor]' do
    expect(chef_run).to_not run_bash('verify-container-executor')
  end

  it 'should create config file sqoop-env.sh' do
    expect(chef_run).to create_template('/etc/sqoop/conf/sqoop-env.sh').with(mode: '0444') 
  end

  it 'should create user hcat' do
    expect(chef_run).to create_user('hcat').with(shell: '/bin/bash', home: '/usr/lib/hcatalog', system: true) 
  end
  
  it 'installs hive packages' do
    %w{hive hcatalog libmysql-java}.each do |pkg|
      expect(chef_run).to upgrade_package(pkg)
    end
  end

  it 'creates link /usr/lib/hive/lib/mysql.jar' do
    expect(chef_run).to create_link('/usr/lib/hive/lib/mysql.jar').with(to: '/usr/share/java/mysql.jar')
  end

  it 'creates directories for HDFS storage' do
    chef_run.node[:bcpc][:hadoop][:mounts].each do |i|
      expect(chef_run).to create_directory("/disk/#{i}/yarn/").with(owner: 'yarn', group: 'yarn', mode: 0755)
      %w{mapred-local local logs}.each do |d|
        expect(chef_run).to create_directory("/disk/#{i}/yarn/#{d}").with(owner: 'yarn', group: 'hadoop', mode: 0755)
      end
    end
  end

  it 'enables and starts nodemanager, hdfs-datanode services' do
    %w{hadoop-yarn-nodemanager hadoop-hdfs-datanode}.each do |svc|
      expect(chef_run).to enable_service(svc)
      expect(chef_run).to start_service(svc)
    end
  end

  it 'nodemanager and hdfs-datanode services subscribes to site.xml changes' do
    %w{hadoop-yarn-nodemanager hadoop-hdfs-datanode}.each do |svc|
      service = chef_run.service(svc)
      expect(service).to subscribe_to('template[/etc/hadoop/conf/hdfs-site.xml]').on(:restart).delayed
      expect(service).to subscribe_to('template[/etc/hadoop/conf/yarn-site.xml]').on(:restart).delayed
    end 
  end

  #context 'when the mounts is less than tolerated' do
  #  let(:chef_run) do
  #    ChefSpec::Runner.new do |node|
  #      node.set[:bcpc][:hadoop][:mounts] = []
  #      node.set[:bcpc][:hadoop][:hdfs][:failed_volumes_tolerated] = 5
  #    end
  #  end
  #
  #  it 'raises an exception' do
  #    chef_run.converge 'bcpc-hadoop::datanode'
  #    expect { chef_run }.to raise_error
  #  end
  #end

  context 'when the mounts is more than tolerated' do
    let(:chef_run) do
      ChefSpec::Runner.new do |node|
        node.set[:bcpc][:hadoop][:mounts] = ["a","b","c"]
        node.set[:bcpc][:hadoop][:hdfs][:failed_volumes_tolerated] = 1
      end
    end

    it 'does not raise an exception' do
      chef_run.converge 'bcpc-hadoop::datanode'
      expect { chef_run }.to_not raise_error
    end
  end
end
