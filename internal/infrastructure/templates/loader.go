package templates

import (
	"embed"
	"fmt"
	"html/template"
	"strings"

	domain "github.com/companyofcreators/mail-service/internal/domain/mail"
)

//go:embed *.html
var templateFS embed.FS

// Load parses all embedded HTML templates and validates them.
func Load() (*template.Template, error) {
	tmpl := template.New("")
	tmpl = tmpl.Funcs(template.FuncMap{
		"safeHTML": func(s string) template.HTML {
			return template.HTML(s)
		},
	})

	entries, err := templateFS.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("read template directory: %w", err)
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("no templates found in embedded filesystem")
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".html") {
			continue
		}

		data, err := templateFS.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read template %s: %w", name, err)
		}

		content := string(data)
		if content == "" {
			return nil, fmt.Errorf("template %s is empty", name)
		}

		tmpl, err = tmpl.New(name).Parse(content)
		if err != nil {
			return nil, fmt.Errorf("parse template %s: %w", name, err)
		}
	}

	// Verify all expected templates are loaded
	requiredTemplates := []string{
		domain.TemplateVerification.TemplateFile(),
		domain.TemplateWelcome.TemplateFile(),
		domain.TemplatePasswordReset.TemplateFile(),
		domain.TemplateNotification.TemplateFile(),
	}

	for _, required := range requiredTemplates {
		if tmpl.Lookup(required) == nil {
			return nil, fmt.Errorf("required template not found: %s", required)
		}
	}

	return tmpl, nil
}
