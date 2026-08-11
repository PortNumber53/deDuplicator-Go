package files

import "testing"

func TestProgressDisplayPolicy(t *testing.T) {
	tests := []struct {
		name        string
		terminal    bool
		environment map[string]string
		want        progressDisplayPolicy
	}{
		{
			name:     "interactive terminal uses progress and color",
			terminal: true,
			want:     progressDisplayPolicy{Visible: true, Color: true},
		},
		{
			name:     "redirected output hides progress",
			terminal: false,
			want:     progressDisplayPolicy{},
		},
		{
			name:        "CI hides progress",
			terminal:    true,
			environment: map[string]string{"CI": "true"},
			want:        progressDisplayPolicy{},
		},
		{
			name:        "Jenkins hides progress",
			terminal:    true,
			environment: map[string]string{"JENKINS_URL": "https://jenkins.example"},
			want:        progressDisplayPolicy{},
		},
		{
			name:        "NO_COLOR keeps progress without ANSI color",
			terminal:    true,
			environment: map[string]string{"NO_COLOR": "1"},
			want:        progressDisplayPolicy{Visible: true, Color: false},
		},
		{
			name:        "dumb terminal keeps progress without ANSI color",
			terminal:    true,
			environment: map[string]string{"TERM": "dumb"},
			want:        progressDisplayPolicy{Visible: true, Color: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := func(key string) string {
				return tt.environment[key]
			}
			if got := progressDisplayPolicyFor(tt.terminal, getenv); got != tt.want {
				t.Fatalf("policy = %+v, want %+v", got, tt.want)
			}
		})
	}
}
