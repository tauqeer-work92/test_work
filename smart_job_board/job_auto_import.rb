class SmartJobBoard::JobAutoImport
  class << self

    def call
      begin
        @broken_imports = []

        all_jobs = get_all_jobs
        if all_jobs.class != Array
          @broken_imports.append({ "error"=> all_jobs, "source"=> "get_all_jobs method"}) unless @broken_imports.present?
          send_error_email
          puts "RUH ROH, AUTO IMPORT ERROR"
          return "RUH ROH, AUTO IMPORT ERROR"
        end
        
        if all_jobs.size == 0
          send_error_email if @broken_imports.present?
          return "no_new_jobs"
        end

        deactivate_all_jobs  # only jobs created via Job Auto Import

        Job.upsert_all(
          all_jobs,
          returning: false,
          unique_by: [:ats_id]
        )

        update_jobs_for_all_employers(all_jobs)

        all_jobs = nil # clear up memory

        set_activation_histories
        #TODO We do need to update this as this job activation history might need to be moved to Jobs_job_boards table
        time = Time.now
        new_jobs = fetch_new_jobs(time)

        puts "latest_15_mins_jobs".concat(new_jobs.size.to_s)
        update_job_fields(new_jobs)
        reindex_jobs(new_jobs)  # this will only run at 10am UTC

        new_jobs = nil  # clear up memory

        puts "ANY BROKEN JOB IMPORTS? #{@broken_imports.present?.to_s}"
        
        send_error_email if @broken_imports.present?
      rescue => e
        puts "GlOBAL ERROR"
        puts e
        @broken_imports.append({ "error"=> e.to_s, "source"=> "call method"})
        send_error_email
      end
    end

    private

    def update_jobs_for_all_employers(all_jobs, employer=nil)
      current_ats_ids = all_jobs.map { |job| job[:ats_id] }
      existing_jobs = Job.where(ats_id: current_ats_ids).pluck(:ats_id, :id).to_h

      associations_for_upsert = []
      employers_to_iterate = employer.nil? ? Employer.all : [employer]

      employers_to_iterate.each do |current_employer|
        board_ids = current_employer.job_boards.pluck(:id)
        employer_jobs = all_jobs.select { |job| job[:employer_id] == current_employer.id }

        employer_jobs.each do |job|
          job_id = existing_jobs[job[:ats_id]]
          next unless job_id

          board_ids.each do |board_id|
            associations_for_upsert << {
                job_id: job_id,
                job_board_id: board_id,
                active: true
            }
          end
        end
      end

      JobJobBoard.upsert_all(associations_for_upsert, unique_by: [:job_id, :job_board_id])
    end

    def fetch_new_jobs(time, employer=nil)
      if employer
        job_board_ids = employer.job_boards.pluck(:id)
      else
        job_board_ids = JobBoard.joins(:employers).pluck(:id).uniq
      end
  
      new_jobs = Job.joins(:job_boards)
                    .where(job_boards: { id: job_board_ids, active: true })
                    .where(posted_by: Job.posted_by_options["Job Auto Import"], created_at: (time - 900)..time)
      new_jobs
    end

    def get_all_jobs
      begin
        employers = Employer.select(:id, :company_name, :email, :apply_url_tracking_params, :ats, :ats_url_param, :ats_key, :remote, :import_jobs, :company_description, :workday_credentials).where(active: true, deleted: false, import_jobs: true).where.not(ats: nil).where.not(ats: "team_tailor")
        all_jobs = get_job_feed_data(employers)
        team_tailor_jobs = get_team_tailor_jobs
        all_jobs.append(team_tailor_jobs) if team_tailor_jobs.class == Array
        all_jobs.flatten!
        puts "ALL JOBS SIZE: " + all_jobs.size.to_s
        all_jobs
      rescue => e
        puts e
        return "get_all_jobs_error"
      end
    end

    def deactivate_all_jobs(employer=nil)
      return unless time_to_deactivate?

      if employer
        deactivate_jobs_for_employer(employer)
      else
        deactivate_jobs_globally()
      end

    rescue => e
      puts e
      @broken_imports.append({ "error"=> e.to_s, "source"=> "deactivate_all_jobs_error" })
    end

    def deactivate_all_jobs_for_employer(employer)
      board_ids = employer.job_boards.pluck(:id)
      JobJobBoard.where(job_board_id: board_ids).update_all(active: false)

      puts "deactivated all Employer jobs | #{Time.now}"
    end

    def deactivate_jobs_globally
      Job.joins(:job_boards).where(job_boards: { active: true }).update_all("job_boards.active = ?", false)
      puts "deactivated all jobs | #{Time.now}"
    end

    def time_to_deactivate?
      [9, 10, 11].include?(Time.now.hour)
    end

    def base_job_scope(employer)
      scope = Job.joins(:job_boards).where(job_boards: { active: true })
      scope.where(employer_id: employer.id)
    end

    def reindex_jobs(new_jobs)
      begin
        time = Time.now
        if [9, 10, 11].include?(time.hour)
          Job.reindex!
        else
          new_jobs.reindex!
        end
      rescue => e
        puts e
        @broken_imports.append({ "error"=> e.to_s, "source"=> "reindex_jobs_error" })
      end
    end

    def update_job_fields(new_jobs)
      begin
        sub_categories_all = SubCategory.includes(:categories).all.to_a
        job_types_all = JobType.all.to_a
        experience_levels_all = ExperienceLevel.all.to_a

        new_jobs.find_each(batch_size: 100) do |job|
          categories_data = SmartJobBoard::CustomFieldsMethods.get_job_sub_categories(job.title, sub_categories_all)
          job_type = job.custom_fields["job_type"].present? ? SmartJobBoard::CustomFieldsMethods.get_job_type(job.custom_fields["job_type"], job_types_all) : SmartJobBoard::CustomFieldsMethods.get_job_type(job.title, job_types_all)
          experience_levels = SmartJobBoard::CustomFieldsMethods.get_experience_levels(job, experience_levels_all)
          
          job_specific_fields = {"categories"=> categories_data['categories'], "sub_categories"=> categories_data['sub_categories'], "job_type"=> job_type, "experience_levels"=> experience_levels}
          SmartJobBoard::CustomFieldsMethods.add_job_custom_fields(job)
          SmartJobBoard::CustomFieldsMethods.add_job_specific_fields(job, job_specific_fields)
          Superlinked::UploadRecordToSuperlinked.call("new_job", {"job_id"=> job.id})
        end
      rescue => e
        puts e
        @broken_imports.append({ "error"=> e.to_s, "source"=> "update_job_fields_error" })
      end
    end

    def set_activation_histories(employer=nil)
      begin
        current_time = Time.now
        jobs = employer.nil? ? Job.where(active: true).where.not(ats_id: nil) : Job.where(employer_id: employer.id, active: true).where.not(ats_id: nil)
        jobs.find_each(batch_size: 100) do |job|
          activation_history = job.activation_history.present? && job.activation_history.has_key?('date_ranges') ? job.activation_history['date_ranges'] : []
          if activation_history.size == 0 || (activation_history[-1]['to'].present? && current_time > activation_history[-1]['to'])  # no history logged or logged expiration date is in the past
            activation_history.append({"from"=> current_time, "to"=> nil})
            job.update(activation_history: {"date_ranges"=> activation_history}, activation_date: current_time, expiration_date: nil)
          else
            next if job.activation_date.present?
            activation_history[-1]['from'] = current_time
            activation_history[-1]['to'] = nil
            job.update(activation_history: {"date_ranges"=> activation_history}, activation_date: current_time, expiration_date: nil) unless activation_history == job.activation_history
          end
        end
      rescue => e
        puts "ERROR SETTING ACTIVATION HISTORIES"
        puts e
        @broken_imports.append({ "error"=> e.to_s, "source"=> "set_activation_histories" })
      end
    end
    
    def get_job_feed_data(employers, employers_only=false)
      begin
        job_batch = []
        employers.each do |employer|
          ats = employer.ats
          puts ats
          case ats
          when "lever"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_lever_jobs(employer.ats_url_param)
            puts employer.email 
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "lever", employer)
            puts "LEVER COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "greenhouse"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_greenhouse_jobs(employer.ats_url_param)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "greenhouse", employer)
            puts "GREENHOUSE COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "workable"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_workable_jobs(employer.ats_url_param)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "workable", employer)
            puts "WORKABLE COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "ashby"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_ashby_jobs(employer.ats_url_param)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "ashby", employer)
            puts "ASHBY COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "recruitee"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_recruitee_jobs(employer.ats_url_param)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "recruitee", employer)
            puts "RECRUITEE COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "sync_hr"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_synchr_jobs(employer.ats_url_param)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "sync_hr", employer)
            puts "SYNCHR COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "clear_company"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_clearcompany_jobs(employer.ats_url_param, employer.apply_url_tracking_params)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "clear_company", employer)
            puts "CLEAR COMPANY COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "breezy_hr"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_breezy_jobs(employer.ats_url_param)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "breezy_hr", employer)
            puts "BREEZY COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "jazz_hr"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_jazz_jobs(employer.ats_key)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "jazz_hr", employer)
            puts "JAZZ COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "rippling"
            jobs = WebScraper::ScrapeAtsJobs.get_rippling_jobs(employer.ats_url_param)
            puts employer.email
            if jobs.class != Hash
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "rippling", employer)
            puts "RIPPLING COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "bamboo_hr"
            jobs = WebScraper::ScrapeAtsJobs.get_bamboo_hr_jobs(employer.ats_url_param)
            if jobs.class != Hash
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            puts employer.email
            transformed_jobs = transform_jobs(jobs, "bamboo_hr", employer)
            puts "BAMBOO COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "personio"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_personio_jobs(employer.ats_url_param)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "personio", employer)
            puts "PERSONIO COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "jobvite"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_jobvite_jobs(employer.ats_url_param)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "jobvite", employer)
            puts "JOBVITE COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "polymer"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_polymer_jobs(employer.ats_url_param)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "polymer", employer)
            puts "POLYMER COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "jobscore"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_jobscore_jobs(employer.ats_url_param)
            puts employer.email
            if jobs == "error_during_api_call"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "jobscore", employer)
            puts "JOBSCORE COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          when "icims"
            jobs = WebScraper::ScrapeAtsJobs.get_icims_jobs(employer.ats_url_param)
            if jobs.class != Array
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            puts employer.email
            transformed_jobs = transform_jobs(jobs, "icims", employer)
            puts "ICIMS COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end

          when "workday"
            jobs = SmartJobBoard::AtsFeedsApiClient.get_workday_jobs(employer)
            puts employer.email 
            if jobs == "error_during_api_call" || jobs == "error_during_authentication"
              @broken_imports.append({ "email"=> employer.email, "error"=> jobs, "ats"=> ats, "source"=> "error_during_api_call" })
              next
            end
            transformed_jobs = transform_jobs(jobs, "workday", employer)
            puts "WORKDAY COUNT " + transformed_jobs.size.to_s
            return transformed_jobs if employers_only
            if transformed_jobs.class == Array
              job_batch.append(transformed_jobs)
            else
              @broken_imports.append({ "email"=> employer.email, "error"=> transformed_jobs.to_s, "ats"=> ats, "source"=> "transform_jobs" })
            end
            
          else
            @broken_imports.append({ "email"=> employer.email, "error"=> "ats_not_recognized", "ats"=> ats, "source"=> "transform_jobs" })
            puts "ats_not_recognized"
          end
        end

        job_batch.flatten
      rescue => e
        puts e
        return "error getting jobs"
      end
    end

    def get_team_tailor_jobs(employer=nil)
      begin
        jobs = SmartJobBoard::AtsFeedsApiClient.get_teamtailor_jobs
        if jobs.class != Array
          @broken_imports.append({ "error"=> jobs.to_s, "ats"=> "team_tailor", "source"=> "error_during_api_call" })
          return
        end

        employers = jobs.map {|j| j['company']}.uniq!
        employer_objects_mapping = {}

        if employer.class == Employer
          employers.each do |e|
            next unless e == employer.company_name
            employer_objects_mapping[e] = employer
            break
          end
        else
          employers.each do |e|
            next unless Employer.exists?(ats: "team_tailor", company_name: e, active: true, deleted: false, import_jobs: true)
            employer_objects_mapping[e] = Employer.where(ats: "team_tailor", company_name: e, active: true, deleted: false, import_jobs: true).last
          end
        end

        transformed_jobs = transform_jobs(jobs, "team_tailor", nil, employer_objects_mapping)
        if transformed_jobs.class != Array
          @broken_imports.append({ "error"=> transformed_jobs.to_s, "ats"=> "team_tailor", "source"=> "transform_jobs" })
          return
        end

        puts "TEAM TAILOR COUNT " + transformed_jobs.size.to_s
        transformed_jobs
      rescue => e
        puts "ERROR GETTING TEAM TAILOR JOBS"
        puts e
        @broken_imports.append({ "error"=> e.to_s, "ats"=> "team_tailor", "source"=> "get_team_tailor_jobs" })
        return
      end
    end

    def transform_jobs(jobs, ats, employer, employer_object_mapping=nil)
      begin
        posted_by = "Job Auto Import"
        select_keys = ['title', 'active', 'how_to_apply', 'description', 'employer_name', 'employer_id', 'location', 'remote', 'ats_id', 'posted_by', 'custom_fields', 'employer_logo']
        case ats
        when "lever"
          ats_id_prefix = "lev_"
          custom_fields = {}

          jobs.map do |job|
            job['title'] = job['text']
            job['how_to_apply'] = job.delete('applyUrl').concat(employer&.apply_url_tracking_params.to_s)
            description_text = set_lever_description(job, employer)
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = description_text
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = employer&.remote || location == "" || location.to_s.downcase.include?("remote") || job['title'].to_s.downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + job.delete('id').to_s
            job['posted_by'] = posted_by
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "greenhouse"
          ats_id_prefix = "gre_"
          custom_fields = {}

          jobs.map do |job|
            job['title'] = job['title']
            job['how_to_apply'] = job.delete('absolute_url').concat(employer&.apply_url_tracking_params.to_s)
            description_text = job.delete("content").to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = employer&.remote || location == "" || location.downcase.include?("remote") || job['title'].to_s.downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + job.delete('id').to_s
            job['posted_by'] = posted_by
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "workable"
          ats_id_prefix = "wor_"

          jobs.map do |job|
            job['title'] = job['title']
            job['how_to_apply'] = job.delete('application_url').concat(employer&.apply_url_tracking_params.to_s)
            description_text = job.delete("description").to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = employer&.remote || location == "" || location.downcase.include?("remote") || job['telecommuting'] == true || job['title'].to_s.downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + job.delete('shortcode').to_s
            job['posted_by'] = posted_by
            custom_fields = {"experience_level"=> {"seniority"=> job['experience']}}
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "ashby"
          ats_id_prefix = "ash_"
          custom_fields = {}

          jobs.map do |job|
            job['title'] = job['title']
            job['how_to_apply'] = job.delete('applyUrl').concat(employer&.apply_url_tracking_params.to_s)
            description_text = job.delete("descriptionHtml").to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = job.delete('isRemote') == true || employer&.remote || location == "" || location.downcase.include?("remote") || job['title'].to_s.downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + job.delete('id').to_s
            job['posted_by'] = posted_by
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "recruitee"
          ats_id_prefix = "rec_"

          jobs.map do |job|
            job['title'] = job['title']
            job['how_to_apply'] = job['careers_apply_url'].concat(employer&.apply_url_tracking_params.to_s)
            description_text = job.delete("description").to_s + job.delete("requirements").to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = job['remote'] == true || employer&.remote || location == "" || location.downcase.include?("remote") || job['title'].to_s.downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + job.delete('id').to_s
            job['posted_by'] = posted_by
            custom_fields = {"experience_level"=> {"seniority"=> job['experience_code']}}
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "team_tailor"
          ats_id_prefix = "tt_"
          custom_fields = {}
          transformed_jobs = []

          jobs.each do |job|
            employer = employer_object_mapping[job['company']]
            next unless employer.class == Employer
            job['title'] = job['title']
            job['how_to_apply'] = job['applyurl']
            description_text = job['description'].to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = ["fully", "hybrid"].include?(job['remotestatus']) || employer&.remote || location == "" || job['title'].to_s.downcase.include?("remote")
            job['ats_id'] = job['referencenumber'].present? ? job['referencenumber'] : ats_id_prefix + "#{Digest::MD5.hexdigest(job['how_to_apply'])}"
            job['posted_by'] = posted_by
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
            transformed_jobs.append(job)
          end
          return transformed_jobs
        when "sync_hr"
          ats_id_prefix = "sync_"
          custom_fields = {}

          jobs.map do |job|
            job['title'] = job['title']
            job['how_to_apply'] = job['link'].concat(employer&.apply_url_tracking_params.to_s)
            description_text = job['description'][1].to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = employer&.remote || location == "" || job['title'].to_s.downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + "#{Digest::MD5.hexdigest(job['link'])}"
            job['posted_by'] = posted_by
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "clear_company"
          ats_id_prefix = "clear_"
          custom_fields = {}

          jobs.map do |job|
            job['title'] = job['PositionTitle']
            job['how_to_apply'] = job['ApplyUrl']
            description_text = job['Description'].to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = employer&.remote || location == "" || job['title'].to_s.downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + "#{Digest::MD5.hexdigest(job['ApplyUrl'])}"
            job['posted_by'] = posted_by
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "breezy_hr"
          ats_id_prefix = "bre_"
          custom_fields = {}

          jobs.map do |job|
            job['title'] = job['name']
            job['how_to_apply'] = job['url'].concat("/apply").concat(employer&.apply_url_tracking_params.to_s)
            description_text = job.delete("description").to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = job['location']['is_remote'] == true || employer&.remote || location == "" || job['title'].to_s.downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + job.delete('id').to_s
            job['posted_by'] = posted_by
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "jazz_hr"
          ats_id_prefix = "jaz_"
          custom_fields = {}

          jobs.map do |job|
            job['title'] = job['title']
            job['how_to_apply'] = "https://#{employer&.ats_url_param}.applytojob.com/apply/#{job['board_code']}?source=climatebase"
            description_text = job.delete("description").to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = employer&.remote || location == "" || location.downcase.include?("remote") || job['title'].downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + job.delete('id').to_s
            job['posted_by'] = posted_by
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "rippling"
          ats_id_prefix = "rip_"
          custom_fields = {}

          transformed_jobs = []
          jobs.keys.each do |k|
            begin
              job = jobs[k]
              job_final = {}
              title_and_location = get_rippling_fields(job)
              description_text = set_rippling_description(job)
              description_text = description_text.present? ? description_text : employer&.company_description.to_s
              job_final['active'] = 1
              job_final['status'] = status
              job_final['employer_name'] = employer&.company_name
              job_final['employer_logo'] = employer&.logo
              job_final['employer_id'] = employer&.id
              job_final['description'] = sanitize_description(description_text)
              apply_url_tracking_params = employer&.apply_url_tracking_params.present? ? employer&.apply_url_tracking_params : "climatebase"
              job_final['how_to_apply'] = "https://#{employer.ats_url_param}.rippling-ats.com#{k}?source=#{apply_url_tracking_params}"
              job_final['title'] = title_and_location['title']
              job_final['location'] = title_and_location['location']
              job_final['remote'] = employer&.remote || title_and_location['location'] == "" || title_and_location['location'].downcase.include?("remote") || title_and_location['title'].downcase.include?("remote")
              job_final['ats_id'] = ats_id_prefix + "#{Digest::MD5.hexdigest(job_final['how_to_apply'])}"
              job_final['posted_by'] = posted_by
              job_final['custom_fields'] = custom_fields
              transformed_jobs.append(job_final)
              job_final = nil  # clear for memory
            rescue => e
              puts e
            end
          end
          if jobs.size != transformed_jobs.size
            @broken_imports.append({ "email"=> employer.email, "error"=> "could_not_successfully_transform_ALL_jobs", "ats"=> ats, "source"=> "transform_jobs" })
          end
          return transformed_jobs
        when "bamboo_hr"
          ats_id_prefix = "bamboo_"
          transformed_jobs = []

          jobs.keys.each do |k|
            begin
              job = jobs[k]['job']
              job_final = {}
              fields = get_bamboo_fields(job, employer)
              description_text = fields['description']
              description_text = description_text.present? ? description_text : employer&.company_description.to_s
              job_final['active'] = 1
              job_final['status'] = status
              job_final['employer_name'] = employer&.company_name
              job_final['employer_logo'] = employer&.logo
              job_final['employer_id'] = employer&.id
              job_final['description'] = description_text
              apply_url_tracking_params = employer&.apply_url_tracking_params.present? ? employer&.apply_url_tracking_params : "climatebase"
              job_final['how_to_apply'] = "https://#{employer.ats_url_param}.bamboohr.com/careers/#{k}?source=#{apply_url_tracking_params}"
              title = jobs[k]['title'].to_s.strip
              location = fields['location'].to_s.strip
              job_final['title'] = title
              job_final['location'] = location
              job_final['remote'] = employer&.remote || !location.present? || location.downcase.include?("remote") || title.downcase.include?("remote")
              job_final['ats_id'] = ats_id_prefix + "#{Digest::MD5.hexdigest(job_final['how_to_apply'])}"
              job_final['posted_by'] = posted_by
              custom_fields = {"job_type"=> fields['job_type'], "experience_level"=> fields['experience_level']}
              job_final['custom_fields'] = custom_fields
              transformed_jobs.append(job_final)
              job_final = nil  # clear for memory
            rescue => e
              puts e
            end
          end
          if jobs.size != transformed_jobs.size
            @broken_imports.append({ "email"=> employer.email, "error"=> "could_not_successfully_transform_ALL_jobs", "ats"=> ats, "source"=> "transform_jobs" })
          end
          return transformed_jobs
        when "personio"
          ats_id_prefix = "perso_"
          jobs.map do |job|
            job['title'] = job['name']
            job['how_to_apply'] = "https://#{employer.ats_url_param}.jobs.personio.de/job/#{job['id']}?language=en&display=en&gh_src=Climatebase#apply"
            description_text = get_personio_description(job)
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = employer&.remote || location == "" || job['title'].to_s.downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + "#{Digest::MD5.hexdigest(job['how_to_apply'])}"
            job['posted_by'] = posted_by
            experience_levels = get_personio_experience_levels(job)
            custom_fields = {"job_type"=> job['schedule'], "experience_level"=> experience_levels}
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "jobvite"
          ats_id_prefix = "jv_"

          jobs.map do |job|
            job['title'] = job['title']
            job['how_to_apply'] = job['detail_url'].concat(employer&.apply_url_tracking_params.to_s)
            description_text = job['description'].to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = employer&.remote || location == "" || location.downcase.include?("remote") || job['title'].to_s.downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + "#{employer.id.to_s.concat(job['id'].to_s)}"
            job['posted_by'] = posted_by
            custom_fields = {"job_type"=> job['jobtype']}
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "polymer"
          ats_id_prefix = "poly_"

          jobs.map do |job|
            job['title'] = job['title']
            job['how_to_apply'] = job.delete('job_post_url').concat(employer&.apply_url_tracking_params.to_s)
            description_text = job.delete("description").to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = employer&.remote || location == "" || ["remote friendly", "remote"].include?(job['remoteness_pretty'].downcase)
            job['ats_id'] = ats_id_prefix + job.delete('id').to_s
            job['posted_by'] = posted_by
            custom_fields = {"job_type"=> job['kind_pretty']}
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "jobscore"
          ats_id_prefix = "js_"

          jobs.map do |job|
            job['title'] = job['title']
            job['how_to_apply'] = job.delete('detail_url').concat(employer&.apply_url_tracking_params.to_s)
            description_text = job.delete("description").to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location
            job['remote'] = employer&.remote || location == "" || job['remote'].to_s.downcase.include?("yes")
            job['ats_id'] = ats_id_prefix + job.delete('id').to_s
            job['posted_by'] = posted_by
            custom_fields = {"job_type"=> job['job_type'], "experience_level"=> {"seniority"=> job['experience_level']}}
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        when "icims"
          ats_id_prefix = "icims_"
          custom_fields = {}
          transformed_jobs = []

          jobs.map do |job|
            job_final = {}
            job_fields = get_icims_fields(job)
            job_final['title'] = job_fields['title']
            job_final['active'] = 1
            job_final['status'] = status
            job_final['how_to_apply'] = job_fields['url']
            description_text = job_fields['description'].to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job_final['description'] = sanitize_description(description_text)
            job_final['employer_name'] = employer&.company_name
            job_final['employer_logo'] = employer&.logo
            job_final['employer_id'] = employer&.id
            job_final['location'] = job_fields['location']
            job_final['remote'] = employer&.remote || job_fields['location'] == "" || job_fields['location'].to_s.downcase.include?("remote")
            job_final['ats_id'] = ats_id_prefix + "#{Digest::MD5.hexdigest(job_fields['url'])}"
            job_final['posted_by'] = posted_by
            job_final['custom_fields'] = custom_fields
            transformed_jobs.append(job_final)
            job_final = nil  # clear for memory
          end
          if jobs.size != transformed_jobs.size
            @broken_imports.append({ "email"=> employer.email, "error"=> "could_not_successfully_transform_ALL_jobs", "ats"=> ats, "source"=> "transform_jobs" })
          end
          return transformed_jobs
        when "workday"
          ats_id_prefix = "workday_"

          jobs.map do |job|
            job['title'] = job['title']
            job['how_to_apply'] = job.delete('url').concat("?source=Climatebase")
            description_text = job.delete("jobDescription").to_s
            description_text = description_text.present? ? description_text : employer&.company_description.to_s
            job['description'] = sanitize_description(description_text)
            job['employer_name'] = employer&.company_name
            job['employer_logo'] = employer&.logo
            job['employer_id'] = employer&.id
            location = get_location(job, ats, employer)
            job['location'] = location.gsub("Remote", "").gsub("-", "").strip
            job['remote'] = employer&.remote || location == "" || location.downcase.include?("remote")
            job['ats_id'] = ats_id_prefix + job.delete('id').to_s
            job['posted_by'] = posted_by
            job_type = job['timeType'].is_a?(Hash) ? job['timeType']['descriptor'] : "Full time"
            custom_fields = {"job_type"=> job_type}
            job['custom_fields'] = custom_fields
            job.select! { |k, v| select_keys.include?(k) }
          end
        else
          return "ats_not_recognized"
        end
      rescue => e
        puts e
        return "error while transforming jobs"
      end
    end

    def get_location(job, ats, employer)  # also returns title for select ATS
      begin
        remote_synonyms = ["anywhere", "talent network", "remote", "distributed", "virtual", "worldwide", "nationwide", "any location", "multiple locations", "add location"]
        case ats
        when "lever"
          if !job.has_key?('categories')
            return ""
          elsif job['categories']['location'].to_s.downcase.include?(" or ") || job['categories']['location'].to_s.downcase.include?("/")
            split_char = ""
            [" or ", "/"].each do |char|
              if job['categories']['location'].to_s.downcase.include?(char)
                split_char = char
                break
              end
            end
            location = ""
            split_locations = job['categories']['location'].split(split_char)
            split_locations.each_with_index do |loc, i|
              if i == (split_locations.size - 1)
                location.concat(split_locations[i].titlecase)
              else
                location.concat(split_locations[i].titlecase).concat(", ")
              end
            end
            return location
          elsif remote_synonyms.any? { |s| job['categories']['location'].to_s.downcase.include?(s) }
            return ""
          elsif job['categories']['location'].class == Hash
            return "" if job['categories']['location']['name'].to_s.downcase.include?("remote")
            return job['categories']['location']['name'].to_s
          else
            return job['categories']['location'].to_s
          end
        when "greenhouse"
          if !job.has_key?('location') || !job['location'].has_key?('name')
            return ""
          elsif job['location']['name'].to_s.downcase.include?(" or ") || job['location']['name'].to_s.downcase.include?("/") || job['location']['name'].to_s.downcase.include?(" and ")
            split_char = ""
            [" or ", "/", " and "].each do |char|
              if job['location']['name'].to_s.downcase.include?(char)
                split_char = char
                break
              end
            end
            location = ""
            split_locations = job['location']['name'].split(split_char)
            split_locations.each_with_index do |loc, i|
              if i == (split_locations.size - 1)
                location.concat(split_locations[i].titlecase)
              else
                location.concat(split_locations[i].titlecase).concat(", ")
              end
            end
            return "Bourne, MA, United States" if location == "Bourne"
            return location
          elsif remote_synonyms.any? { |s| job['location']['name'].to_s.downcase.include?(s) }
            location = job['location']['name'].downcase.include?("remote") ? job['location']['name'] : ""
            return location
          else
            check = custom_location_fields(job, employer)
            return check['location'] if check["needs_custom_location"] == true
            return job['location']['name'].to_s
          end
        when "workable"
          if job['country'] == "United States"
            if !job['city'].present?
              return job['country'] unless job['state'].present?
              return job['state'].concat(", ").concat(job['country'])
            else
              return job['city'].concat(", ").concat(job['country']) unless job['state'].present?
              return job['city'].concat(", ").concat(job['state']).concat(", ").concat(job['country'])
            end
            
            return job['city'].to_s.concat(", ").concat(job['country']) if job['state'] == ""
          elsif job['country'].present?
            return job['country'] if !job['city'].present?
            return job['city'].concat(", ").concat(job['country'])
          else
            return ""
          end
        when "ashby"
          if job['location'].to_s.include?("/") || job['location'].to_s.include?(" or ")
            split_char = ""
            [" or ", "/"].each do |char|
              if job['location'].to_s.downcase.include?(char)
                split_char = char
                break
              end
            end
            location = ""
            split_locations = job['location'].split(split_char)
            split_locations.each_with_index do |loc, i|
              if i == (split_locations.size - 1)
                location.concat(split_locations[i].titlecase)
              else
                location.concat(split_locations[i].titlecase).concat(", ")
              end
            end
            return location
          elsif remote_synonyms.any? { |s| job['location'].to_s.downcase.include?(s) }
            return ""
          else
            return job['location'].to_s
          end
        when "recruitee"
          if job['location'].to_s.include?("/") || job['location'].to_s.include?(" or ")
            split_char = ""
            [" or ", "/"].each do |char|
              if job['location'].to_s.downcase.include?(char)
                split_char = char
                break
              end
            end
            location = ""
            split_locations = job['location'].split(split_char)
            split_locations.each_with_index do |loc, i|
              if i == (split_locations.size - 1)
                location.concat(split_locations[i].titlecase)
              else
                location.concat(split_locations[i].titlecase).concat(", ")
              end
            end
            return location
          elsif remote_synonyms.any? { |s| job['location'].to_s.downcase.include?(s) }
            return ""
          else
            return job['location'].to_s
          end
        when "team_tailor"
          locations = get_teamtailor_location_object(job['locations'])
          return "" unless locations.present?

          if job['remotestatus'] == "fully"
            location = locations['country'].to_s
            return location
          else
            location = ""
            city = locations['city']
            country = locations['country']
            location.concat(city) if city.class == String
            if country.class == String && country.present? && location.present?
              location.concat(", #{country}")
              return location
            elsif country.class == String && country.present?
              return country
            end
            return location
          end
        when "sync_hr"
          return "" if !job['location'].present? || job['location'].downcase.include?("n/a")
          return job['location']
        when "clear_company"
          location = ""
          location.concat("#{job['City']}, ") if job['City'].present? && !job['City'].include?("i:nil")
          location.concat("#{job['CountrySubdivisionName']}, ") if job['CountrySubdivisionName'].present? && !job['CountrySubdivisionName'].include?("i:nil")
          location.concat(job['CountryCode']) if job['CountryCode'].present?
          return location
        when "breezy_hr"
          location = job['location']['name']
          location.concat(", #{job['location']['country']['name']}") unless location == job['location']['country']['name']
          return location
        when "jazz_hr"
          location = ""
          location.concat("#{job['city']}, ") if job['city'].present?
          location.concat("#{job['state']}, ") if job['state'].present?
          location.concat(job['country_id']) if job['country_id'].present?
          return location
        when "personio"
          if ["extern", "remote"].include?(job['office'].to_s.downcase)
            return ""
          else
            return job['office']
          end
        when "jobvite"
          if job['location'].present?
            return job['location']
          else
            return ""
          end
        when "polymer"
          location = ""
          location.concat("#{job['city']}, ") if job['city'].present?
          location.concat("#{job['state_region']}, ") if job['state_region'].present?
          location.concat(job['country']) if job['country'].present?
          return location
        when "jobscore"
          location = ""
          return location if job['city'].to_s.downcase == "remote"
          location.concat("#{job['city']}, ") if job['city'].present?
          location.concat("#{job['state']}, ") if job['state'].present?
          location.concat(job['country']) if job['country'].present?
          return location
        when "icims"
          location = ""
          location.concat("#{job['jobLocation'][0]['address']['addressLocality']}, ") if job['jobLocation'][0]['address']['addressLocality'].present? && job['jobLocation'][0]['address']['addressLocality'] != "UNAVAILABLE"
          location.concat("#{job['jobLocation'][0]['address']['addressRegion']}, ") if job['jobLocation'][0]['address']['addressRegion'].present? && job['jobLocation'][0]['address']['addressRegion'] != "UNAVAILABLE"
          location.concat(job['jobLocation'][0]['address']['addressCountry']) if job['jobLocation'][0]['address']['addressCountry'].present? && job['jobLocation'][0]['address']['addressCountry'] != "UNAVAILABLE"
          return location
        when "workday"
          return "" if job['id'].present? && job['primaryLocation'].blank?  # if job['id'] is present, then job data is present, just doesn't have location
          return job['primaryLocation']['descriptor']  # in the future, look at 'additionalLocations' key
        else
          puts "ats_not_recognized"
          return ""
        end
      rescue => e
        puts "GET LOCATION ERROR"
        puts e
        @broken_imports.append({ "email"=> employer&.email, "error"=> e.to_s, "ats"=> ats, "source"=> "get_location" }) unless @broken_imports.include?({ "email"=> employer&.email, "error"=> e.to_s, "ats"=> ats, "source"=> "get_location" })
        return ""
      end
    end

    def get_teamtailor_location_object(locations)
      return unless locations.is_a?(Hash) || locations.is_a?(Array)
    
      if locations.is_a?(Hash) && locations['country'].present?
        return locations
      end
    
      if locations.is_a?(Array)
        locations.each do |element|
          result = get_teamtailor_location_object(element)
          return result if result
        end
      elsif locations.is_a?(Hash)
        locations.each_value do |value|
          result = get_teamtailor_location_object(value)
          return result if result
        end
      end
    
      nil
    end
    

    def get_rippling_fields(job)
      begin
        title_and_location = job.xpath("//div[@class='job-title-container']")
        title = ""
        location = ""
        title_and_location.children.each do |c|
          title = c.children.to_s if c.name == "h1" || c.name == "h2"
          location = c.children.children.first.to_s if c.name == "div"
          location.tr!("\t", "")
          location.tr!("\n", "")
          location.strip!
        end
        return {"title"=> title, "location"=> location}
      rescue => e
        puts "GET RIPPLING FIELDS ERROR"
        puts e
        @broken_imports.append({ "error"=> e.to_s, "source"=> "get_rippling_fields" }) unless @broken_imports.include?({ "error"=> e.to_s, "source"=> "get_rippling_fields" })
        return
      end
    end

    def get_icims_fields(job)  # url, description, title, location
      begin
        fields = JSON.parse job.children[1].children[3].children[11].children.text

        if fields['jobLocation'].class == Array && fields['jobLocation'][0].class == Hash && fields['jobLocation'][0]['address'].class == Hash
          location = get_location(fields, "icims", nil)
        else
          location = job.xpath("//div[@class='col-xs-6 header left']").children[3].text
          location.tr!("\n", "")
        end

        description = fields['description']
        description.tr!("\n", "")

        {"url"=> fields['url'], "description"=> description, "title"=> fields['title'], "location"=> location}
      rescue => e
        puts "GET iCIMS FIELDS ERROR"
        puts e
        @broken_imports.append({ "error"=> e.to_s, "source"=> "get_icims_fields" }) unless @broken_imports.include?({ "error"=> e.to_s, "source"=> "get_icims_fields" })
        return
      end
    end

    def set_lever_description(job, employer_id)
      begin
        do_not_sanitize = [1131712, 1131415]  # IDs of employers who we should not HTML sanitize description for
        description_text = ""
        if job.include?("description")
          description_text.concat(job.delete("description").to_s.concat("<br><br>"))
        end
        if job.include?('lists')
          job.delete('lists').each do |list|
            if list.has_key?('text') && list.has_key?('content')
              description_text.concat('<h3>').concat(list["text"]).concat('</h3><ul>').concat(list["content"]).concat('</ul>')
            end
          end
        end
        description_text.concat(job["descriptionPlain"]) unless description_text.present?
        job.delete("descriptionPlain")
        if job.include?("additional")
          description_text.concat("<br>").concat(job.delete("additional"))
          job.delete("additionalPlain")
        elsif job.include?("additionalPlain")
          description_text.concat("<br>").concat(job.delete("additionalPlain"))
        end
        return description_text if do_not_sanitize.include?(employer_id)
        sanitize_description(description_text)
      rescue => e
        puts "LEVER DESCRIPTION ERROR"
        puts e
        @broken_imports.append({ "error"=> e.to_s, "source"=> "get_lever_description" }) unless @broken_imports.include?({ "error"=> e.to_s, "source"=> "get_lever_description" })
        return ""
      end
    end

    def set_rippling_description(job)
      begin
        job_content_body = job.xpath("//div[@class='job-content-body user-content']")
        description = ""
        job_content_body.children.each do |c|
          next if c.name == "text"
          description.concat(c.to_s)
        end
        sanitize_description(description)
      rescue => e
        puts "RIPPLING DESCRIPTION ERROR"
        puts e
        @broken_imports.append({ "error"=> e.to_s, "source"=> "get_rippling_description" }) unless @broken_imports.include?({ "error"=> e.to_s, "source"=> "get_rippling_description" })
        return ""
      end
    end

    def set_bamboo_description(description_element)
      begin
        description = ""
        description_element.each do |c|
          next if c.name == "text"
          description.concat(c.to_s)
        end
        description.gsub!(/\r/, '')
        
        sanitize_description(description)
      rescue => e
        puts "BAMBOO DESCRIPTION ERROR"
        puts e
        @broken_imports.append({ "error"=> e.to_s, "source"=> "get_bamboo_description" }) unless @broken_imports.include?({ "error"=> e.to_s, "source"=> "get_bamboo_description" })
        return ""
      end
    end

    def get_bamboo_fields(job, employer, fields_index = 0)
      begin
        fields = {}

        description_element = job.css('#descriptionWrapper')
        description = set_bamboo_description(description_element)
        fields['description'] = description

        field_elements = job.xpath("//*[text() = 'Location']")
        return fields unless field_elements.present?

        field_elements[fields_index].parent.parent.children.each do |c|
          field_name = c.children[0].text
          case field_name
          when "Location"
            fields['location'] = c.children[1].text
            next
          when "Minimum Experience"
            fields['experience_level'] = c.children[1].text
            next
          when "Employment Type"
            fields['job_type'] = c.children[1].text
            next
          else
            next
          end
        end
        
        fields
      rescue => e
        puts "GET BAMBOO FIELDS ERROR"
        puts e
        if fields_index >= 0 && fields_index < 2
          fields_index += 1
          puts fields_index
          return get_bamboo_fields(job, employer, fields_index)
        else
          @broken_imports.append({ "error"=> e.to_s, "source"=> "get_bamboo_fields", "email"=> employer.email }) unless @broken_imports.include?({ "error"=> e.to_s, "source"=> "get_bamboo_fields", "email"=> employer.email })
          return
        end
      end
    end

    def get_personio_description(job)
      begin
        description = ""
        job['jobDescriptions']['jobDescription'].each do |d|
          description.concat("#{d['name']}<br>#{d['value']}")
        end
        sanitize_description(description)
      rescue => e
        puts e
        return ""
      end
    end

    def get_personio_experience_levels(job)
      begin
        years_exp = job['yearsOfExperience'].class == String ? job['yearsOfExperience'].split("-") : []

        years_exp[0] = years_exp[0].to_i if years_exp[0].present?
        years_exp[1] = years_exp[1].to_i if years_exp[1].present?
        
        if years_exp.present? && (years_exp[0].class == Integer || years_exp[1].class == Integer)
          return {"min_years"=> years_exp[0], "max_years"=> years_exp[1]}
        else
          return {"seniority"=> job['seniority']}
        end

      rescue => e
        puts e
        @broken_imports.append({ "error"=> e.to_s, "source"=> "get_personio_experience_levels" }) unless @broken_imports.include?({ "error"=> e.to_s, "source"=> "get_personio_experience_levels" })
      end
    end

    def sanitize_description(description)
      begin
        description = ActionController::Base.helpers.sanitize(CGI.unescapeHTML(description), {:tags=>['div', 'b', 'em', 'strong', 'a', 'p', 'br', 'span', 'ul', 'ol', 'li', 'h1', 'h2', 'h3', 'hr', 'img'], :attributes=>['href', 'style', 'src']})
        description.gsub!(/\n/, '')
        description
      rescue => e
        puts "SANITIZE DESCRIPTION ERROR"
        puts e
        @broken_imports.append({ "error"=> e.to_s, "source"=> "sanitize_description" }) unless @broken_imports.include?({ "error"=> e.to_s, "source"=> "sanitize_description" })
        return description
      end
    end

    def custom_location_fields(job, employer)
      begin
        if employer&.id == 1131507 && job['location']['name'].to_s.downcase.include?("headquarter")
          return {"needs_custom_location"=> true, "location"=> "Berkeley, CA, USA"}
        end
        return {"needs_custom_location"=> false}
      rescue => e
        puts e
        return {"needs_custom_location"=> false}
      end
    end

    def send_error_email
      error_params = {"broken_imports"=> @broken_imports}
      Emails::InternalNotificationBrokenJobImportJob.perform_later error_params unless Rails.env.test?
    end

  end
end
