package cmd

import (
	"sync"

	"github.com/devopsext/eye/common"
	"github.com/devopsext/eye/model"
	"github.com/spf13/cobra"
)

var v1ModelOptions = model.V1ModelOptions{
	File: envGet("V1_MODEL_FILE", "").(string),
}

func NewTrainCommand(wg *sync.WaitGroup) *cobra.Command {

	trainCmd := cobra.Command{
		Use:   "train",
		Short: "Train model",
	}

	trainV1ModelCmd := cobra.Command{
		Use:   "v1",
		Short: "Train v1 model",
		Run: func(cmd *cobra.Command, args []string) {

			obs := common.NewObservability(logs, metrics)

			v1 := model.NewV1Model(v1ModelOptions, obs)
			v1.Train(wg)

			wg.Wait()
		},
	}

	flags := trainV1ModelCmd.PersistentFlags()
	flags.StringVar(&v1ModelOptions.File, "v1-model-file", v1ModelOptions.File, "V1 model file path")
	trainCmd.AddCommand(&trainV1ModelCmd)

	return &trainCmd
}
