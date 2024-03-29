//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <memory>
#include <utility>
<<<<<<< HEAD
=======

<<<<<<< HEAD
>>>>>>> b9de15364ba6ead2b5d77e96d7b17730c13c3d5f

=======
>>>>>>> e393a17c88d170ab3f568ae59df3cd955fa8d858
namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) {
    rows = r;
    cols = c;
    linear = new T[r * c];
  }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() { delete[] linear; }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    data_ = new T *[r];
    for (int i = 0; i < r; i++) {
      data_[i] = this->linear + i * this->cols;
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return this->rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return this->cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override { return data_[i][j]; }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override { data_[i][j] = val; }

  // TODO(P0): Add implementation
  void MatImport(T *arr) override { memcpy(this->linear, arr, (this->rows * this->cols) * sizeof(T)); }

  // TODO(P0): Add implementation
  ~RowMatrix() override { delete[] data_; };

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
<<<<<<< HEAD
    // TODO(P0): Add code
=======
>>>>>>> b9de15364ba6ead2b5d77e96d7b17730c13c3d5f
    int rows_1 = mat1->GetRows();
    int rows_2 = mat2->GetRows();
    int cols_1 = mat1->GetColumns();
    int cols_2 = mat2->GetColumns();
<<<<<<< HEAD
    std::unique_ptr<RowMatrix<int>> result{new RowMatrix<int>(rows_1, cols_1)};

    if (rows_1 != rows_2 || cols_1 != cols_2) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    for (int i = 0; i < rows_1; i++) {
      for (int j = 0; j < cols_1; j++) {
        T val = mat1->GetElem(i, j) + mat2->GetElem(i, j);
        result->SetElem(i, j, val);
      }
    }
=======

    if (rows_1 != rows_2 || cols_1 != cols_2) {
      return nullptr;
    }

    std::unique_ptr<RowMatrix<T>> result{new RowMatrix<T>(rows_1, cols_1)};
    for (int i = 0; i < rows_1; i++) {
      for (int j = 0; j < cols_1; j++) {
        int val = mat1->GetElem(i, j) + mat2->GetElem(i, j);
        result->SetElem(i, j, val);
      }
    }

>>>>>>> b9de15364ba6ead2b5d77e96d7b17730c13c3d5f
    return result;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
<<<<<<< HEAD
    // TODO(P0): Add code
=======
>>>>>>> b9de15364ba6ead2b5d77e96d7b17730c13c3d5f
    int rows_1 = mat1->GetRows();
    int rows_2 = mat2->GetRows();
    int cols_1 = mat1->GetColumns();
    int cols_2 = mat2->GetColumns();
<<<<<<< HEAD
    std::unique_ptr<RowMatrix<int>> result{new RowMatrix<int>(rows_1, cols_2)};
=======
>>>>>>> b9de15364ba6ead2b5d77e96d7b17730c13c3d5f

    if (cols_1 != rows_2) {
      return nullptr;
    }

<<<<<<< HEAD
=======
    std::unique_ptr<RowMatrix<T>> result{new RowMatrix<T>(rows_1, cols_2)};
>>>>>>> b9de15364ba6ead2b5d77e96d7b17730c13c3d5f
    for (int i = 0; i < rows_1; i++) {
      for (int j = 0; j < cols_2; j++) {
        result->SetElem(i, j, 0);
        for (int k = 0; k < cols_1; k++) {
          int val = result->GetElem(i, j) + mat1->GetElem(i, k) * mat2->GetElem(k, j);
          result->SetElem(i, j, val);
        }
      }
    }
<<<<<<< HEAD
=======

>>>>>>> b9de15364ba6ead2b5d77e96d7b17730c13c3d5f
    return result;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
<<<<<<< HEAD
    // TODO(P0): Add code
    int rows_A = matA->GetRows();
    int cols_A = matA->GetColumns();

    int rows_B = matB->GetRows();
    int cols_B = matB->GetColumns();

    int rows_C = matC->GetRows();
    int cols_C = matC->GetColumns();

    if (cols_A != rows_B) {
      return nullptr;
    }

    if (rows_A != rows_C || cols_B != cols_C) {
      return nullptr;
    }

    std::unique_ptr<RowMatrix<int>> intermediate{new RowMatrix<int>(rows_C, cols_C)};
    std::unique_ptr<RowMatrix<int>> result{new RowMatrix<int>(rows_C, cols_C)};

    intermediate = MultiplyMatrices(std::move(matA), std::move(matB));
    result = AddMatrices(std::move(intermediate), std::move(matC));

=======
    int rows_a = matA->GetRows();
    int rows_b = matB->GetRows();
    int cols_a = matA->GetColumns();
    int cols_b = matB->GetColumns();
    int rows_c = matC->GetRows();
    int cols_c = matC->GetColumns();

    if (cols_a != rows_b || rows_a != rows_c || cols_b != cols_c) {
      return nullptr;
    }

    std::unique_ptr<RowMatrix<T>> result{new RowMatrix<T>(rows_c, cols_c)};
    std::unique_ptr<RowMatrix<T>> result_add{new RowMatrix<T>(rows_c, cols_c)};

    result_add = MultiplyMatrices(std::move(matA), std::move(matB));
    result = AddMatrices(std::move(result_add), std::move(matC));
>>>>>>> b9de15364ba6ead2b5d77e96d7b17730c13c3d5f
    return result;
  }
};
}  // namespace bustub
