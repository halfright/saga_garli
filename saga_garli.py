# Use Saga to submit Garli pbs jobs for bootstrap analysis
# Takes local nexus files and garli.conf template
# Creates config files, copies input to remote directory, then submits BOOTSTRAP_REP*len(nexus_files) jobs
# Finally, retrieves *.boot.tre files from remote cluster
# Written by Brad Nelson for Scientific Computing

import os, sys, time
import bliss.saga as saga
import paramiko

BOOTSTRAP_REP = 2

def main():
  # make configuration files
    try: 
    	template = open("./garli.conf", 'r')
    	nexus_files = [o.rsplit('.',1)[0] for o in os.listdir('.') if o.endswith('.nex')]
    	for nexus in nexus_files:
    	    if os.path.exists(nexus):
    	    	pass
            else: os.mkdir(nexus)
            outconf = open("./%s.conf" % nexus, 'w')
            for line in template:
				if line.find("datafname") != -1:
					lineout = "datafname = %s.nex\n" % nexus
				elif line.find("ofprefix") != -1:
					lineout = "ofprefix = %s\n" % nexus
				elif line.find("availablememory") != -1:
					lineout = "availablememory = 1024\n"
				elif line.find("writecheckpoints") != -1:
					lineout = "writecheckpoints = 1\n"
				elif line.find("outgroup") != -1:
					lineout = "outgroup = 161-180\n"   # Specific to my datasets, but only affects how they are displayed later
				elif line.find("searchreps") != -1:
					lineout = "searchreps = 4\n"       # Normally this would be higher to get a better ML tree
				elif line.find('bootstrapreps') != -1:
					lineout = 'bootstrapreps = 1\n'
				else: lineout = line
				outconf.write(lineout)
            outconf.close()
            template.seek(0)
        template.close()
    except:
        print "An error occurred during config creation"
        sys.exit(-1)
        
    # define context and sessions    
    try:	
        ctx = saga.Context()
        ctx.type = saga.Context.SSH
        ctx.userid = 'bnelson'
        ses = saga.Session()
        ses.contexts.append(ctx)
        js_pbs = saga.job.Service('pbs+ssh://tezpur.hpc.lsu.edu', session=ses)
    except saga.Exception, ex:
        print "An error occured during context and session creation"
        sys.exit(-1)
    
    # create connection to remote server  
    try:
    	remotehost = 'tezpur.hpc.lsu.edu'
        dirlocal = '/Users/bnels21/Dropbox/SagaGarli/'
        dir_dest = '/work/bnelson/tmp/'
        dir = 'sftp://%s%s' % (remotehost, dir_dest)
        workdir = saga.filesystem.Directory(dir, session = ses)
    except IOError, ex:
        print "An error occured during job execution: %s" % (str(ex))
        print "Error during file scp"
        sys.exit(-1)
    
    # create directories on remote host and copy input files
    try:
    	for file in nexus_files:
            for x in range(0, BOOTSTRAP_REP):
                if not workdir.exists('BS_%s' % x):
                    workdir.make_dir('BS_%s' % x)
                if not workdir.exists('BS_%s/%s.nex' % (x, file)):
                    os.system('scp ./%s.nex %s@%s:%sBS_%s/'  % (file, ctx.userid, remotehost, dir_dest, x))
                if not workdir.exists('BS_%s/%s.conf' % (x, file)):
                    os.system('scp ./%s.conf %s@%s:%sBS_%s/' % (file, ctx.userid, remotehost, dir_dest, x))	
        print "Finished copying files to remote host"
    except saga.Exception, ex:
		print "An error occured during copy job execution: %s" % (str(ex))
		sys.exit(-1)

    # queue garli jobs    
    try: 
    	jobs = []
    	for file in nexus_files:
			for x in range(0, BOOTSTRAP_REP):
				jd = saga.job.Description()
				jd.queue = 'checkpt'
				jd.wall_time_limit = 15
				jd.total_cpu_count = 1
				jd.working_directory = '%s/BS_%s' % (workdir.get_url().path, x)
				jd.executable        = '/home/bnelson/Garli-2.0'
				jd.arguments         = ['%s.conf' % file] # Number of times to run conf file
				job = js_pbs.create_job(jd)
				job.run()
				jobs.append(job)
				print ' * Submitted %s to Jobs.' % job.jobid
				
        counter = 0
        while len(jobs) > 0:
			for job in jobs:
				jobstate = job.get_state()
				print ' * Job %s status: %s' % (job.jobid, jobstate)
				if jobstate is saga.job.Job.Done:
					jobs.remove(job)
					counter += 1
			print '%d/%d jobs completed' % (counter, len(nexus_files) * BOOTSTRAP_REP)
			time.sleep(30)
    except saga.Exception, ex:
        print "An error occurred during garli job execution: %s" % (str(ex))
        sys.exit(-1)
			
    # retrieve files from server
    for file in nexus_files:
		for x in range(0, BOOTSTRAP_REP):
			try:
				outfilesource = '%sBS_%s/%s.boot.tre' % (dir, x, file)
				outfiletarget = 'sftp://localhost%s%s/%s%s.boot.tre' % (dirlocal, file, file, x)
				out = saga.filesystem.File(outfilesource, session=ses)
				out.copy(outfiletarget)
			except saga.Exception, ex:
				print "An error occurred during %s%d file retrieval: %s" % (file, x, str(ex))
				continue
			print "Staged out %s to %s (size: %s bytes)" % (outfilesource, outfiletarget, out.get_size())

if __name__ == "__main__":
    main()
