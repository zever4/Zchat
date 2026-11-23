package workers

import (
	"context"
	"log"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

// unused now because message id is needed for frontend to work properly.
// maybe create new workers that will take any task specifiend in args as function
func StartDBWorker(ctx context.Context, repo repository.MessageRepository, jobs <-chan *domain.Message) {
for {
	select{
	case msg, ok := <-jobs:
		if !ok {
			return
		}

		func() {
			ctx, cancel := context.WithTimeout(ctx, 5 * time.Second)
			defer cancel()

			err := repo.Save(ctx, msg)
			if err != nil {
				log.Printf("Error saving message: %v", err)
			}
		}()
		case <-ctx.Done():
			return
		}
	}
}