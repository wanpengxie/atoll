package lagoon

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type ObsChannelRow struct {
	ID             channel.ID            `json:"id"`
	ParentID       channel.ID            `json:"parent_id,omitempty"`
	Name           string                `json:"name"`
	QualifiedName  string                `json:"qualified_name"`
	Type           string                `json:"type"`
	Status         regspec.ChannelStatus `json:"status"`
	OwnerPrincipal string                `json:"owner_principal"`
	CreatedAt      int64                 `json:"created_at"`
}

type ObsPrincipalRow struct {
	ID          string                  `json:"id"`
	Kind        actor.Kind              `json:"kind"`
	Email       string                  `json:"email,omitempty"`
	DisplayName string                  `json:"display_name,omitempty"`
	Status      regspec.PrincipalStatus `json:"status"`
	CreatedAt   int64                   `json:"created_at"`
}

type ObsDaemonRow struct {
	ID             string               `json:"id"`
	OwnerPrincipal string               `json:"owner_principal"`
	Name           string               `json:"name"`
	Status         regspec.DeviceStatus `json:"status"`
	CreatedAt      int64                `json:"created_at"`
}

type ObsDeclRow struct {
	ID           string             `json:"id"`
	Name         string             `json:"name"`
	Description  string             `json:"description,omitempty"`
	Owner        string             `json:"owner"`
	DefaultClass string             `json:"default_class"`
	Config       json.RawMessage    `json:"config,omitempty"`
	Status       regspec.DeclStatus `json:"status"`
	Visibility   string             `json:"visibility"`
	CreatedAt    int64              `json:"created_at"`
	UpdatedAt    int64              `json:"updated_at"`
}

func (r *Registry) ObsChannels(ctx context.Context, parent *string) ([]ObsChannelRow, bool, error) {
	rows, err := r.ListPresentChannels(ctx)
	if err != nil && len(rows) == 0 {
		return nil, false, err
	}
	complete := err == nil
	out := make([]ObsChannelRow, 0, len(rows))
	for _, row := range rows {
		if parent == nil {
			if row.ParentID != "" {
				continue
			}
		} else if string(row.ParentID) != *parent {
			continue
		}
		out = append(out, projectObsChannel(row))
	}
	return out, complete, nil
}

func (r *Registry) ObsChannel(ctx context.Context, id string) (ObsChannelRow, bool, error) {
	row, found, err := r.GetChannelDesired(ctx, channel.ID(id))
	if err != nil || !found || row.Status != regspec.ChannelPresent {
		return ObsChannelRow{}, found && row.Status == regspec.ChannelPresent, err
	}
	return projectObsChannel(row), true, nil
}

func (r *Registry) ObsPrincipals(ctx context.Context) ([]ObsPrincipalRow, bool, error) {
	rows, err := r.ListPrincipals(ctx)
	if err != nil && len(rows) == 0 {
		return nil, false, err
	}
	complete := err == nil
	out := make([]ObsPrincipalRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, ObsPrincipalRow{
			ID: row.ID, Kind: row.Kind, Email: row.Email, DisplayName: row.DisplayName,
			Status: row.Status, CreatedAt: row.CreatedAt,
		})
	}
	return out, complete, nil
}

func (r *Registry) ObsDaemons(ctx context.Context) ([]ObsDaemonRow, bool, error) {
	rows, err := r.ListDevices(ctx)
	if err != nil && len(rows) == 0 {
		return nil, false, err
	}
	complete := err == nil
	out := make([]ObsDaemonRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, ObsDaemonRow{
			ID: row.ID, OwnerPrincipal: row.OwnerPrincipal, Name: row.Name,
			Status: row.Status, CreatedAt: row.CreatedAt,
		})
	}
	return out, complete, nil
}

func (r *Registry) ObsDecls(ctx context.Context) ([]ObsDeclRow, bool, error) {
	rows, err := r.ListDecls(ctx)
	if err != nil && len(rows) == 0 {
		return nil, false, err
	}
	complete := err == nil
	out := make([]ObsDeclRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, ObsDeclRow{
			ID: row.ID, Name: row.Name, Description: row.Description, Owner: row.Owner, DefaultClass: row.DefaultClass,
			Config: append(json.RawMessage(nil), row.Config...), Status: row.Status,
			Visibility: row.Visibility, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
		})
	}
	return out, complete, nil
}

func projectObsChannel(row regspec.ChannelRow) ObsChannelRow {
	return ObsChannelRow{
		ID: row.ID, ParentID: row.ParentID, Name: row.Name, QualifiedName: row.QualifiedName,
		Type: row.Type, Status: row.Status, OwnerPrincipal: row.OwnerPrincipal, CreatedAt: row.CreatedAt,
	}
}
