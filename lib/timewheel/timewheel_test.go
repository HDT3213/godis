package timewheel

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTimeWheelConcurrency(t *testing.T) {
	// Initialize the time wheel
	tw := New(time.Second, 3600)
	tw.Start()
	defer tw.Stop()

	var wg sync.WaitGroup
	const jobCount = 1000

	// Function to simulate a job
	job := func(id int) {
		fmt.Printf("Job %d executed at %v\n", id, time.Now())
	}

	// Add jobs concurrently
	wg.Add(jobCount)
	for i := 0; i < jobCount; i++ {
		go func(id int) {
			defer wg.Done()
			delay := time.Duration(id%10) * time.Second // Randomize delays
			Delay(delay, fmt.Sprintf("job-%d", id), func() { job(id) })
		}(i)
	}

	// Remove jobs concurrently
	wg.Add(jobCount / 10)
	for i := 0; i < jobCount; i += 10 {
		go func(id int) {
			defer wg.Done()
			time.Sleep(2 * time.Second) // Ensure some jobs are added before canceling
			Cancel(fmt.Sprintf("job-%d", id))
		}(i)
	}

	// Add timed jobs with specific `At` time
	wg.Add(jobCount / 10)
	for i := 0; i < jobCount; i += 10 {
		go func(id int) {
			defer wg.Done()
			at := time.Now().Add(5 * time.Second)
			At(at, fmt.Sprintf("timed-job-%d", id), func() {
				fmt.Printf("Timed Job %d executed at %v\n", id, time.Now())
			})
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	fmt.Println("All tasks submitted and executed/cancelled successfully.")
}

func TestTimeWheelConcurrentAddRunRemove(t *testing.T) {
	// Initialize the time wheel
	tw := New(time.Millisecond*100, 360)
	tw.Start()
	defer tw.Stop()

	var wg sync.WaitGroup
	const totalJobs = 1000

	// Function to simulate a job
	job := func(id int) {
		fmt.Printf("Job %d executed at %v\n", id, time.Now())
	}

	// Concurrently add jobs
	wg.Add(totalJobs)
	for i := 0; i < totalJobs; i++ {
		go func(id int) {
			defer wg.Done()
			delay := time.Duration(id%50) * time.Millisecond // Randomize delays
			Delay(delay, fmt.Sprintf("job-%d", id), func() { job(id) })
		}(i)
	}

	// Concurrently remove some jobs
	wg.Add(totalJobs / 5)
	for i := 0; i < totalJobs; i += 5 {
		go func(id int) {
			defer wg.Done()
			time.Sleep(time.Millisecond * 10) // Allow some jobs to be added first
			Cancel(fmt.Sprintf("job-%d", id))
		}(i)
	}

	// Concurrently add and execute timed jobs
	wg.Add(totalJobs / 10)
	for i := 0; i < totalJobs; i += 10 {
		go func(id int) {
			defer wg.Done()
			at := time.Now().Add(time.Millisecond * time.Duration(20+id%30))
			At(at, fmt.Sprintf("timed-job-%d", id), func() {
				fmt.Printf("Timed Job %d executed at %v\n", id, time.Now())
			})
		}(i)
	}

	// Concurrently add long-duration jobs and immediately remove them
	wg.Add(totalJobs / 20)
	for i := 0; i < totalJobs; i += 20 {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("long-job-%d", id)
			Delay(time.Second*5, key, func() { fmt.Printf("Long Job %d executed\n", id) })
			time.Sleep(time.Millisecond * 50)
			Cancel(key)
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()
	fmt.Println("Concurrent Add, Run, and Remove Test completed successfully.")
}
