package pipeline

import "strings"

type ValidationWrapper struct {
	repo Repository
}

func (v *ValidationWrapper) GetJobs(in *GetJobsInput) ([]*Job, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}
	return v.repo.GetJobs(in)
}
func (v *ValidationWrapper) CreateJob(in *CreateJobInput) (JobID, error) {
	if err := in.Validate(); err != nil {
		return 0, err
	}
	return v.repo.CreateJob(in)
}

func (v *ValidationWrapper) UpdateJob(in *UpdateJobInput) error {
	if err := in.Validate(); err != nil {
		return err
	}
	return v.repo.UpdateJob(in)
}

func (v *ValidationWrapper) GetRuns(in *GetRunsInput) ([]*Run, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}
	return v.repo.GetRuns(in)
}
func (v *ValidationWrapper) CreateRun(in *CreateRunInput) (RunID, error) {
	if err := in.Validate(); err != nil {
		return 0, err
	}
	return v.repo.CreateRun(in)
}

func (v *ValidationWrapper) UpdateRun(in *UpdateRunInput) error {
	if err := in.Validate(); err != nil {
		return err
	}
	return v.repo.UpdateRun(in)
}

func (in *GetJobsInput) Validate() error {
	if len(in.JobIDs) == 0 {
		return ErrFieldRequired{"JobIDs"}
	}
	return nil
}

func (in *CreateJobInput) Validate() error {
	var errs []error
	if in.Name == "" {
		errs = append(errs, ErrFieldRequired{"Name"})
	}

	if errs != nil {
		return ValidationErrors(errs)
	}
	return nil
}

func (in *UpdateJobInput) Validate() error {
	var errs []error

	if in.Name != nil && *in.Name == "" {
		errs = append(errs, ErrFieldRequired{"Name"})
	}

	//TODO: check cron?

	if len(errs) > 0 {
		return ValidationErrors(errs)
	}
	return nil
}

type ErrFieldRequired struct {
	FieldName string
}

func (e ErrFieldRequired) Error() string {
	return "Field '" + e.FieldName + "' is required"
}

type ValidationErrors []error

func (v ValidationErrors) Error() string {
	errStrs := make([]string, len(v))
	for i, e := range v {
		errStrs[i] = e.Error()
	}
	return strings.Join(errStrs, "\n")
}

func (in *GetRunsInput) Validate() error {
	return nil
}

func (in *CreateRunInput) Validate() error {
	return nil
}

func (in *UpdateRunInput) Validate() error {
	return nil
}
